/**
 * GraphenMail — client-side JS
 * Task polling, sidebar toggle, utilities.
 */

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

    function setProgress(percent, message) {
        progressSection.style.display = '';
        if (progressBar) progressBar.style.width = `${percent}%`;
        if (progressText) progressText.textContent = message;
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
        }, 1500);
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
            setProgress(task.percent || 0, task.error || 'Campaign failed.');
            return;
        }

        if (task.status === 'completed') {
            setStatus('done');
            setProgress(100, task.message || 'Completed!');
            reloadWithoutTaskParam();
            return;
        }

        const status = task.message && task.message.startsWith('Generating')
            ? 'generating'
            : 'crawling';
        setStatus(status);
        setProgress(task.percent || 0, task.message || 'Running...');
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
        if (!alwaysVisible) {
            progressSection.style.display = 'none';
            if (progressBar) progressBar.style.width = '0%';
            return;
        }

        setProgress(0, 'No active task - click a button below to start.');
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
