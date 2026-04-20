/**
 * GraphenMail — client-side JS
 * Task polling, sidebar toggle, utilities.
 */

// ─── Reusable toast helper ─────────────────────────────────────────────
// Usage: showToast("Saved!"), showToast("Oops", "error"), etc.
window.showToast = function(message, type = "success", duration = 4000) {
    const el = document.createElement("div");
    el.className = `gm-toast gm-toast-${type}`;
    el.textContent = message;
    document.body.appendChild(el);
    // Force layout so transition triggers
    requestAnimationFrame(() => el.classList.add("visible"));
    setTimeout(() => {
        el.classList.remove("visible");
        setTimeout(() => el.remove(), 400);
    }, duration);
};

// ─── Sidebar Toggle (Mobile) ───────────────────────────────────────────

document.addEventListener('DOMContentLoaded', function() {
    const toggle = document.getElementById('sidebar-toggle');
    const sidebar = document.getElementById('sidebar');

    if (toggle && sidebar) {
        const overlay = document.createElement('div');
        overlay.className = 'sidebar-overlay';
        document.body.appendChild(overlay);

        toggle.addEventListener('click', () => {
            sidebar.classList.toggle('open');
            overlay.classList.toggle('active');
        });

        overlay.addEventListener('click', () => {
            sidebar.classList.remove('open');
            overlay.classList.remove('active');
        });
    }

    // Flash auto-dismiss
    const flashes = document.querySelectorAll('.flash');
    flashes.forEach(flash => {
        setTimeout(() => {
            flash.style.opacity = '0';
            flash.style.transition = 'opacity 0.3s';
            setTimeout(() => flash.remove(), 300);
        }, 5000);
    });
});

// ─── Task Polling (Campaign generation/crawling) ───────────────────────

function pollTasks() {
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const progressSection = document.getElementById('progress-section');
    const statusBadge = document.getElementById('campaign-status-badge');

    if (!progressSection) return;

    const campaignId = Number(progressSection.dataset.campaignId || 0);
    let taskId = progressSection.dataset.taskId || '';
    let hasReloaded = false;

    function setStatus(status) {
        if (!statusBadge) return;
        statusBadge.textContent = status;
        statusBadge.className = `badge badge-${status}`;
    }

    const progressEta = document.getElementById('progress-eta');
    const startedAt = progressSection.dataset.startedAt || '';

    function computeEta(percent, task) {
        if (!task || task.status !== 'running') return '';
        if (!startedAt || !percent || percent <= 0 || percent >= 100) return '';
        const startMs = Date.parse(startedAt);
        if (!startMs) return '';
        const elapsed = Date.now() - startMs;
        if (elapsed < 3000) return '';
        const totalEst = elapsed / (percent / 100);
        const remainMs = totalEst - elapsed;
        if (remainMs < 1000) return '';
        const secs = Math.round(remainMs / 1000);
        if (secs < 60) return `~${secs}s left`;
        const mins = Math.round(secs / 60);
        if (mins < 60) return `~${mins} min left`;
        const hrs = Math.floor(mins / 60);
        const rem = mins % 60;
        return `~${hrs}h ${rem}m left`;
    }

    function setProgress(percent, message, task = null) {
        progressSection.style.display = '';
        if (progressBar) progressBar.style.width = `${percent}%`;
        if (progressText) progressText.textContent = message;
        if (progressEta) progressEta.textContent = computeEta(percent, task);
    }

    function reloadWithoutTaskParam() {
        if (hasReloaded) return;
        hasReloaded = true;

        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.delete('campaign_task');

        setTimeout(() => {
            const target = nextUrl.toString();
            if (target === window.location.href) {
                window.location.reload();
            } else {
                window.location.replace(target);
            }
        }, 500);
    }

    function pickLatestCampaignTask(tasks) {
        return tasks
            .filter(task => task.task_type === 'campaign' && task.campaign_id === campaignId)
            .sort((a, b) => String(a.started_at || '').localeCompare(String(b.started_at || '')))
            .pop() || null;
    }

    function handleTask(task) {
        if (!task) {
            return;
        }

        taskId = task.task_id || taskId;
        progressSection.dataset.taskId = taskId;

        if (task.status === 'failed') {
            setStatus('failed');
            setProgress(task.percent || 0, task.error || 'Campaign failed.', task);
            return;
        }

        if (task.status === 'completed') {
            setStatus('done');
            setProgress(100, task.message || 'Completed!', task);
            reloadWithoutTaskParam();
            return;
        }

        if (task.status === 'cancelled') {
            setStatus('cancelled');
            setProgress(task.percent || 0, task.message || 'Cancelled.', task);
            reloadWithoutTaskParam();
            return;
        }

        let status = 'running';
        if (task.message && task.message.startsWith('Generating URLs')) {
            status = 'generating';
        } else if (task.message && task.message.startsWith('Crawling URLs')) {
            status = 'crawling';
        } else if (task.message && task.message.startsWith('Extracting Emails')) {
            status = 'crawling';
        }
        setStatus(status);
        setProgress(task.percent || 0, task.message || 'Running...', task);
        setTimeout(poll, 1500);
    }

    // Also set a safety timeout — if no task response after 30s, reload
    let lastActivity = Date.now();
    const safetyInterval = setInterval(() => {
        if (hasReloaded) { clearInterval(safetyInterval); return; }
        if (Date.now() - lastActivity > 30000) {
            clearInterval(safetyInterval);
            window.location.reload();
        }
    }, 5000);

    function poll() {
        if (hasReloaded) return;
        lastActivity = Date.now();

        if (taskId) {
            fetch(`/api/tasks/${taskId}`)
                .then(r => (r.ok ? r.json() : Promise.reject()))
                .then(handleTask)
                .catch(() => {
                    taskId = '';
                    progressSection.dataset.taskId = '';
                    setTimeout(poll, 1500);
                });
            return;
        }

        fetch('/api/tasks')
            .then(r => r.json())
            .then(tasks => {
                const latest = pickLatestCampaignTask(tasks);
                handleTask(latest);
            })
            .catch(() => setTimeout(poll, 5000));
    }

    poll();
}

// ─── Verification Task Polling ─────────────────────────────────────────

// Track completed task IDs so we reload only once
const _seenCompletedTasks = new Set();

function pollVerification() {
    const progressSection = document.getElementById('verify-progress');
    if (!progressSection) return;

    const progressBar = progressSection.querySelector('.progress-bar');
    const progressText = progressSection.querySelector('.progress-text');
    let hasReloaded = false;
    let idleRetries = 0;
    const MAX_IDLE_RETRIES = 20; // 20 × 1.5s = 30 seconds of waiting

    function poll() {
        if (hasReloaded) return;

        fetch('/api/tasks')
            .then(r => r.json())
            .then(tasks => {
                const verifyTasks = tasks.filter(t => t.task_type === 'verification');
                const running = verifyTasks.filter(t => t.status === 'running');
                const latest = running.length > 0 ? running[running.length - 1] : null;

                if (latest) {
                    idleRetries = 0;
                    progressSection.style.display = '';
                    if (progressBar) progressBar.style.width = latest.percent + '%';
                    if (progressText) progressText.textContent = latest.message || 'Verifying...';
                    setTimeout(poll, 1500);
                } else {
                    // Check for newly completed tasks (not already seen)
                    const completed = verifyTasks.filter(
                        t => t.status === 'completed' && !_seenCompletedTasks.has(t.task_id)
                    );

                    if (completed.length > 0) {
                        const last = completed[completed.length - 1];
                        progressSection.style.display = '';
                        if (progressBar) progressBar.style.width = '100%';
                        if (progressText) progressText.textContent = last.message || 'Completed!';

                        // Mark as seen and reload once
                        completed.forEach(t => _seenCompletedTasks.add(t.task_id));
                        hasReloaded = true;
                        setTimeout(() => location.reload(), 2000);
                    } else if (idleRetries < MAX_IDLE_RETRIES) {
                        // Keep polling — task may still be starting
                        idleRetries++;
                        setTimeout(poll, 1500);
                    }
                }
            })
            .catch(() => {
                if (!hasReloaded) setTimeout(poll, 5000);
            });
    }

    // Seed the seen set with already-completed tasks, then always start polling
    fetch('/api/tasks')
        .then(r => r.json())
        .then(tasks => {
            tasks
                .filter(t => t.task_type === 'verification' && t.status === 'completed')
                .forEach(t => _seenCompletedTasks.add(t.task_id));

            // Always poll — a task may be starting right now after form submit
            poll();
        })
        .catch(() => poll());
}

function pollVerificationTask() {
    const progressSection = document.getElementById('verify-progress');
    if (!progressSection) return;

    const progressBar = progressSection.querySelector('.progress-bar');
    const progressText = progressSection.querySelector('.progress-text');
    const alwaysVisible = progressSection.dataset.alwaysVisible === 'true';
    let taskId = progressSection.dataset.taskId || new URL(window.location.href).searchParams.get('verification_task') || '';
    let hasReloaded = false;

    function setProgress(percent, message) {
        progressSection.style.display = '';
        if (progressBar) progressBar.style.width = `${percent}%`;
        if (progressText) progressText.textContent = message;
    }

    function resetProgress() {
        progressSection.style.display = 'none';
        if (progressBar) progressBar.style.width = '0%';
    }

    function reloadWithoutTaskParam() {
        if (hasReloaded) return;
        hasReloaded = true;

        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.delete('verification_task');

        setTimeout(() => {
            const target = nextUrl.toString();
            if (target === window.location.href) {
                window.location.reload();
            } else {
                window.location.replace(target);
            }
        }, 1500);
    }

    function pickLatestVerificationTask(tasks) {
        return tasks
            .filter(t => t.task_type === 'verification')
            .sort((a, b) => String(a.started_at || '').localeCompare(String(b.started_at || '')))
            .pop() || null;
    }

    function handleTask(task) {
        if (!task) {
            resetProgress();
            return;
        }

        taskId = task.task_id;

        if (task.status === 'failed') {
            setProgress(task.percent || 0, task.error || 'Verification failed.');
            return;
        }

        if (task.status === 'completed') {
            setProgress(100, task.message || 'Completed!');
            reloadWithoutTaskParam();
            return;
        }

        setProgress(task.percent || 0, task.message || 'Verifying...');
        setTimeout(poll, 1500);
    }

    function poll() {
        if (hasReloaded) return;

        if (taskId) {
            fetch(`/api/tasks/${taskId}`)
                .then(r => (r.ok ? r.json() : Promise.reject()))
                .then(handleTask)
                .catch(() => {
                    taskId = '';
                    setTimeout(poll, 1500);
                });
            return;
        }

        fetch('/api/tasks')
            .then(r => r.json())
            .then(tasks => {
                const latest = pickLatestVerificationTask(tasks);
                handleTask(latest && latest.status === 'running' ? latest : null);
            })
            .catch(() => setTimeout(poll, 5000));
    }

    poll();
}

// ─── Select All Toggle ─────────────────────────────────────────────────

function toggleAll(source) {
    const checkboxes = document.querySelectorAll('input[name="email_ids"]');
    checkboxes.forEach(cb => cb.checked = source.checked);
}
