/*
 * Shared launch flow used by the homepage (index.html) and the profile page
 * (profile.html) so the "launch an instance" UX never forks again.
 *
 * Exposes a React hook:  window.useLaunchFlow(opts) -> { startLaunch, launchModal, launchStatus, readyUrl, closeLaunchModal }
 *   - startLaunch(requestUrl, body, target): POSTs the launch request then polls
 *     /api/notebook/status, driving a progress modal until ready/failed.
 *   - launchModal: the modal element to render somewhere in the tree.
 *   - opts.onReady(data, target):   called once when status === 'ready'
 *   - opts.onClose(lastStatus):     called when the user closes the modal
 *   - opts.allocatingMessage:       initial modal message
 *
 * The "ready" result is type-aware:
 *   - notebook/opencode  -> "Open Notebook" button (Jupyter)
 *   - gradio/streamlit/comfyui -> "Open App" button
 *   - vllm/sglang (API)  -> OpenAI-compatible endpoint card, no open button
 *   - custom / SSH-only  -> NO web "Open" button (the lab URL would 404); the
 *     SSH access card is shown instead. This fixes the old profile behaviour
 *     that always opened /instances/<id>/lab regardless of type.
 */
(function () {
  const STYLE_ID = 'oneclick-launch-flow-style';
  const CSS = [
    '.launch-modal-body{padding:8px 4px 4px;}',
    '.launch-modal-progress{margin-bottom:20px;}',
    '.launch-modal-stage{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px;font-weight:700;color:#3a4256;}',
    '.launch-modal-pct{color:#8a93a6;font-weight:600;font-size:13px;}',
    '.launch-modal-status{display:flex;align-items:flex-start;gap:0;padding:16px;border-radius:14px;background:#f6f8fd;border:1px solid #eaeefb;}',
    '.launch-modal-status.ready{background:#f3fbf5;border-color:#cdeed5;}',
    '.launch-modal-status.failed{background:#fff7f7;border-color:#ffd9d9;}',
    '.launch-modal-title{font-weight:800;color:#273049;font-size:15px;}',
    '.launch-modal-detail{margin-top:6px;color:#6b758c;line-height:1.5;font-size:13px;}',
    '.launch-modal-technical{margin-top:14px;color:#6b758c;font-size:13px;}',
    '.launch-modal-technical summary{cursor:pointer;color:#566175;font-weight:700;}',
    '.launch-modal-technical-body{margin-top:8px;padding:10px 12px;border-radius:10px;background:#f5f6fa;white-space:pre-wrap;word-break:break-word;color:#7b8496;}',
    '.api-access-card{margin-top:16px;padding:14px 16px;border:1px solid #d6e4ff;border-radius:14px;background:#f5f9ff;}',
    '.api-access-title{font-weight:800;color:#273049;margin-bottom:10px;}',
    '.api-access-row{display:flex;align-items:center;gap:10px;margin-bottom:8px;}',
    '.api-access-label{width:88px;color:#6b758c;font-size:13px;flex:0 0 auto;}',
    '.api-access-row code{background:#eef2fb;padding:2px 8px;border-radius:6px;word-break:break-all;font-size:13px;}',
    '.api-access-curl summary{cursor:pointer;color:#566175;font-weight:700;font-size:13px;margin-top:4px;}',
    '.api-access-pre{margin:0;padding:10px 12px;border-radius:10px;background:#0f1830;color:#cdd6f4;overflow:auto;font-size:12px;white-space:pre;}',
  ].join('');
  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const s = document.createElement('style');
    s.id = STYLE_ID;
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  const STEP_ITEMS = [
    { title: 'Requested' }, { title: 'Scheduling' }, { title: 'Preparing' }, { title: 'Starting' }, { title: 'Ready' },
  ];
  const STEP_PCT = [12, 34, 58, 82, 100];

  function stepIndex(data) {
    if (!data) return 0;
    if (data.status === 'ready') return 4;
    if (data.status === 'jupyter_starting' || data.status === 'running') return 3;
    if (data.status === 'initializing' || data.status === 'loading') return 2;
    if (data.status === 'pending') return 1;
    return 0;
  }
  function statusMessage(data) {
    if (!data) return 'Checking instance status...';
    if (data.reason === 'Unschedulable') return 'Waiting for available GPU resources';
    if (data.reason === 'ImagePullBackOff' || data.reason === 'ErrImagePull') return 'Image pull failed';
    if (data.reason === 'ContainerCreating') return 'Preparing your workspace';
    if (data.reason === 'JupyterStarting') return 'Jupyter is starting';
    if (data.status === 'ready') return 'Instance is ready';
    if (data.status === 'failed') return data.message || 'Instance failed';
    if (data.status === 'not_found') return data.message || 'Instance not found';
    if (data.status === 'pending') return 'Waiting for resources';
    return data.message || 'Checking instance status...';
  }
  function statusDetail(data) {
    if (!data) return '';
    if (data.reason === 'Unschedulable') return 'The cluster has not found a suitable GPU node yet. You can keep waiting or destroy and retry later.';
    if (data.status === 'failed') return data.detail || data.reason || '';
    if (data.status === 'ready') return 'Your instance is ready.';
    return data.reason && data.reason !== data.message ? data.reason : '';
  }

  function renderApiAccessCard(data) {
    const { Typography } = antd; const { Paragraph } = Typography;
    const e = React.createElement;
    const baseUrl = data.api_base_url; const apiKey = data.api_key; const model = data.api_model;
    const modelVal = model || '<model>';
    const curl = 'curl ' + baseUrl + '/chat/completions \\\n  -H "Authorization: Bearer ' + (apiKey || '<API_KEY>') + '" \\\n  -H "Content-Type: application/json" \\\n  -d \'{"model":"' + modelVal + '","messages":[{"role":"user","content":"Hello"}]}\'';
    return e('div', { className: 'api-access-card' },
      e('div', { className: 'api-access-title' }, 'OpenAI-compatible endpoint'),
      e('div', { className: 'api-access-row' }, e('span', { className: 'api-access-label' }, 'Base URL'), e(Paragraph, { copyable: { text: baseUrl }, style: { margin: 0 } }, e('code', null, baseUrl))),
      model && e('div', { className: 'api-access-row' }, e('span', { className: 'api-access-label' }, 'Model'), e(Paragraph, { copyable: { text: model }, style: { margin: 0 } }, e('code', null, model))),
      apiKey && e('div', { className: 'api-access-row' }, e('span', { className: 'api-access-label' }, 'API Key'), e(Paragraph, { copyable: { text: apiKey }, style: { margin: 0 } }, e('code', null, apiKey))),
      e('details', { className: 'api-access-curl' }, e('summary', null, 'Quickstart (curl)'), e(Paragraph, { copyable: { text: curl }, style: { margin: '8px 0 0' } }, e('pre', { className: 'api-access-pre' }, curl)))
    );
  }
  function renderSshAccessCard(data) {
    if (!data || !data.ssh_command) return null;
    const { Typography } = antd; const { Paragraph } = Typography;
    const e = React.createElement;
    return e('div', { className: 'api-access-card' },
      e('div', { className: 'api-access-title' }, 'SSH access'),
      e('div', { className: 'api-access-row' }, e('span', { className: 'api-access-label' }, 'Command'), e(Paragraph, { copyable: { text: data.ssh_command }, style: { margin: 0 } }, e('code', null, data.ssh_command))),
      e('div', { className: 'api-access-row' }, e('span', { className: 'api-access-label' }, 'Host : Port'), e(Paragraph, { copyable: { text: (data.ssh_host || '') + ' ' + (data.ssh_port || '') }, style: { margin: 0 } }, e('code', null, (data.ssh_host || '') + ' : ' + (data.ssh_port || '')))),
      e('div', { style: { color: '#6b758c', fontSize: 12, marginTop: 4 } }, 'Key-based login using the SSH public key from your Profile.')
    );
  }

  window.useLaunchFlow = function (opts) {
    opts = opts || {};
    ensureStyle();
    const { Modal, Button, Progress, Spin } = antd;
    const e = React.createElement;
    const [launchStatus, setLaunchStatus] = React.useState(null);
    const [launchModalOpen, setLaunchModalOpen] = React.useState(false);
    const [readyUrl, setReadyUrl] = React.useState(opts.initialReadyUrl || '');
    const timerRef = React.useRef(null);

    function stopTimer() { if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; } }
    React.useEffect(() => stopTimer, []);

    function poll(initialUrl, target) {
      let url = initialUrl || '';
      let attempts = 0;
      stopTimer();
      timerRef.current = setInterval(async () => {
        attempts += 1;
        try {
          const res = await fetch('/api/notebook/status');
          const data = await res.json();
          if (data.url) url = data.url;
          const merged = Object.assign({}, data, { url, target });
          setLaunchStatus(merged);
          if (data.status === 'ready') {
            stopTimer(); setReadyUrl(url); antd.message.success('Instance is ready');
            if (opts.onReady) opts.onReady(merged, target);
          } else if (data.status === 'failed') {
            stopTimer(); antd.message.error(statusMessage(data));
            if (opts.onFinish) opts.onFinish(merged, 'failed', target);
          } else if (data.status === 'not_found') {
            stopTimer(); antd.message.error(data.message || 'Instance not found');
            if (opts.onFinish) opts.onFinish(merged, 'not_found', target);
          }
          if (attempts >= 240) {
            stopTimer();
            setLaunchStatus({ status: 'pending', reason: 'LongRunning', message: 'Launch is taking longer than expected. You can keep waiting from Profile.', target });
          }
        } catch (err) { /* transient: keep polling */ }
      }, 3000);
    }

    function startLaunch(requestUrl, body, target) {
      target = target || {};
      setReadyUrl('');
      setLaunchModalOpen(true);
      setLaunchStatus({ status: 'allocating', message: opts.allocatingMessage || 'Allocating resources for your instance...', target });
      fetch(requestUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) })
        .then(async (res) => {
          const data = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(data.detail || 'Launch failed');
          poll(data.url, target);
        })
        .catch((err) => {
          setLaunchStatus({ status: 'failed', message: err.message || 'Launch failed', target });
          if (opts.onFinish) opts.onFinish(null, 'failed', target);
        });
    }

    function closeLaunchModal() {
      setLaunchModalOpen(false);
      if (opts.onClose) opts.onClose(launchStatus);
    }
    async function cancelLaunch() {
      try {
        const res = await fetch('/api/notebook/current', { method: 'DELETE' });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || 'Cancel failed');
        stopTimer(); setLaunchStatus(null); setLaunchModalOpen(false); setReadyUrl('');
        antd.message.success(data.message || 'Launch cancelled');
        if (opts.onCancel) opts.onCancel();
      } catch (err) { antd.message.error(err.message || 'Cancel failed'); }
    }

    function renderLaunchModal() {
      const data = launchStatus || {};
      const failed = data.status === 'failed' || data.status === 'not_found';
      const itype = (data.instance_type || '').trim();
      const isApi = !!data.api_base_url || ['vllm', 'sglang'].includes(itype);
      const isApp = ['gradio', 'streamlit', 'comfyui'].includes(itype);
      const isCustom = itype === 'custom';
      // A web "Open" action only makes sense for notebook/app instances. Custom /
      // SSH-only images do not serve the Jupyter lab URL, so we never auto-open it.
      const canOpenWeb = !isApi && !isCustom;
      const openUrl = data.url || readyUrl;
      const ready = data.status === 'ready' && (openUrl || data.api_base_url || data.ssh_command || isCustom);
      const current = stepIndex(data);
      const detail = statusDetail(data);
      const openLabel = isApp ? 'Open App' : 'Open Notebook';
      const kindNoun = isApi ? 'API' : (isApp ? 'app' : (isCustom ? 'instance' : 'workspace'));
      const title = ready
        ? (isApi ? 'Your API is ready' : (isApp ? 'Your app is ready' : (isCustom ? 'Your instance is ready' : 'Your workspace is ready')))
        : statusMessage(data);
      const footer = ready
        ? ((canOpenWeb && openUrl)
            ? [e(Button, { key: 'close', onClick: closeLaunchModal }, 'Close'), e(Button, { key: 'open', type: 'primary', href: openUrl, target: '_blank', onClick: closeLaunchModal }, openLabel)]
            : [e(Button, { key: 'close', type: 'primary', onClick: closeLaunchModal }, 'Done')])
        : failed
          ? [e(Button, { key: 'close', type: 'primary', onClick: () => { setLaunchStatus(null); closeLaunchModal(); } }, 'Close')]
          : [e(Button, { key: 'bg', onClick: closeLaunchModal }, 'Continue in background'), e(Button, { key: 'cancel', danger: true, onClick: cancelLaunch }, 'Cancel launch')];
      return e(Modal, {
        open: launchModalOpen,
        title: ready ? title : ('Starting your ' + kindNoun),
        width: 560, maskClosable: false, keyboard: false,
        onCancel: ready || failed ? closeLaunchModal : undefined,
        closable: ready || failed,
        footer,
      },
        e('div', { className: 'launch-modal-body' },
          e('div', { className: 'launch-modal-progress' },
            e('div', { className: 'launch-modal-stage' },
              e('span', null, failed ? 'Failed' : (ready ? 'Ready' : ((STEP_ITEMS[current] && STEP_ITEMS[current].title) || 'Requested'))),
              e('span', { className: 'launch-modal-pct' }, failed ? '' : (STEP_PCT[current] || 0) + '%')
            ),
            e(Progress, { percent: failed ? 100 : (STEP_PCT[current] || 0), status: failed ? 'exception' : (ready ? 'success' : 'active'), showInfo: false })
          ),
          e('div', { className: 'launch-modal-status ' + (failed ? 'failed' : (ready ? 'ready' : '')) },
            (!ready && !failed) && e(Spin, { size: 'small', style: { marginRight: 10 } }),
            e('div', null, e('div', { className: 'launch-modal-title' }, title), detail && e('div', { className: 'launch-modal-detail' }, detail))
          ),
          (ready && isApi) && renderApiAccessCard(data),
          (ready && data.ssh_command) && renderSshAccessCard(data),
          (data.detail && data.detail !== detail) && e('details', { className: 'launch-modal-technical' },
            e('summary', null, 'Technical details'), e('div', { className: 'launch-modal-technical-body' }, data.detail))
        )
      );
    }

    return {
      launchStatus,
      launchModalOpen,
      readyUrl,
      startLaunch,
      closeLaunchModal,
      cancelLaunch,
      launchModal: renderLaunchModal(),
    };
  };
})();
