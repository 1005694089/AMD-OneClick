/*
 * Shared template editor fields used by every place a template is created/edited:
 *   - index.html  "Publish Template"   (editors -> Gallery)
 *   - profile.html "My Templates"      (editors -> Gallery, users -> private)
 *   - admin.html  "Add/Edit Template"  (admin -> any)
 *
 * Single source of truth for the Deploy Type driven conditional fields so the
 * three forms can never drift again. Rendered as a React component INSIDE an
 * antd <Form>; the parent owns the Form instance, submit handler and modal.
 *
 * Usage:
 *   React.createElement(window.TemplateFormFields, { form, imageOptions, isAdmin })
 *   - form:         the antd form instance (Form.useForm())
 *   - imageOptions: [{ value, label }] of selectable container images
 *   - isAdmin:      when true also shows slug / sort_order / enabled controls
 */
(function () {
  const { Form, Input, Select, Switch, Space, Alert, InputNumber } = antd;

  const APP_PORT_DEFAULTS = { gradio: 7860, streamlit: 8501, comfyui: 8188, vllm: 8000, sglang: 30000 };
  const APP_START_DEFAULTS = {
    gradio: 'python app.py',
    streamlit: 'streamlit run app.py',
    comfyui: 'python main.py --listen 0.0.0.0 --port 8188',
    vllm: 'vllm serve <model> --host 0.0.0.0 --port 8000',
    sglang: 'python -m sglang.launch_server --model <model> --host 0.0.0.0 --port 30000',
  };
  const INSTANCE_TYPE_OPTIONS = [
    { value: 'opencode', label: 'Notebook (Jupyter / OpenCode)' },
    { value: 'gradio', label: 'Gradio App' },
    { value: 'streamlit', label: 'Streamlit App' },
    { value: 'comfyui', label: 'ComfyUI (one-click)' },
    { value: 'vllm', label: 'vLLM Model API' },
    { value: 'sglang', label: 'SGLang Model API' },
    { value: 'custom', label: 'Custom Image (image-defined start command)' },
  ];
  const PORT_OPTIONS = [7860, 8501, 8188, 8000, 30000].map((p) => ({ value: p, label: String(p) }));

  function TemplateFormFields(props) {
    const e = React.createElement;
    const form = props.form;
    const imageOptions = props.imageOptions || [];
    const isAdmin = !!props.isAdmin;
    // canPublish = may list a template in the public Gallery (admins + editors).
    // Everyone else can only create Private (Profile-only) templates.
    const canPublish = isAdmin || !!props.canPublish;
    const t = Form.useWatch('instance_type', form) || 'opencode';

    const isApiType = ['vllm', 'sglang'].includes(t);
    const isAppType = ['gradio', 'streamlit', 'comfyui'].includes(t) || isApiType;
    const isNotebookType = ['opencode', 'jupyter'].includes(t);
    const needsGithub = ['gradio', 'streamlit', 'opencode', 'jupyter'].includes(t);

    return e(React.Fragment, null,
      isAdmin
        ? e('div', { style: { display: 'grid', gridTemplateColumns: 'minmax(0,1fr) 220px', gap: 16 } },
            e(Form.Item, { name: 'title', label: 'Title', rules: [{ required: true, message: 'Title is required' }] }, e(Input, { placeholder: '10-minute Skill Agent' })),
            e(Form.Item, { name: 'slug', label: 'Slug', rules: [{ required: true, message: 'Slug is required' }] }, e(Input, { placeholder: 'skill-agent' }))
          )
        : e(Form.Item, { name: 'title', label: 'Title', rules: [{ required: true, message: 'Title is required' }] }, e(Input, { placeholder: '10-minute Skill Agent' })),
      e(Form.Item, { name: 'description', label: 'Description' }, e(Input.TextArea, { autoSize: true, placeholder: 'Short user-facing description' })),
      e(Space, { style: { width: '100%' }, size: 16, align: 'start' },
        e(Form.Item, { name: 'category', label: 'Category', style: { width: 180 } }, e(Input, { placeholder: 'LLM' })),
        e(Form.Item, { name: 'tags', label: 'Tags', style: { flex: 1, minWidth: 220 } }, e(Input, { placeholder: 'tutorial, Qwen3, agent' })),
        isAdmin && e(Form.Item, { name: 'sort_order', label: 'Sort', style: { width: 90 } }, e(InputNumber, { style: { width: '100%' }, precision: 0 }))
      ),
      e(Form.Item, { name: 'image', label: 'Container Image', rules: [{ required: true, message: 'Image is required' }] },
        e(Select, { options: imageOptions, showSearch: true, optionFilterProp: 'label', popupMatchSelectWidth: true, listHeight: 360, placeholder: 'Select a container image' })),
      e(Form.Item, { name: 'instance_type', label: 'Deploy Type', tooltip: 'Notebook opens Jupyter; app types open a web app; API types serve an OpenAI-compatible endpoint; Custom runs the image\u2019s own start command.' },
        e(Select, { options: INSTANCE_TYPE_OPTIONS, placeholder: 'Notebook (Jupyter / OpenCode)' })),
      isApiType && e(Form.Item, { name: 'model_source', label: 'Model Source', tooltip: 'Where the model weights download from. ModelScope is usually faster in China.' },
        e(Select, { options: [{ value: 'huggingface', label: 'HuggingFace (mirror)' }, { value: 'modelscope', label: 'ModelScope' }], placeholder: 'HuggingFace (mirror)' })),
      isApiType && e(Alert, { type: 'info', showIcon: true, style: { marginBottom: 16 }, message: 'Model API: set the serve command (including the model). Users get an OpenAI-compatible base URL + API key. Serves at /spaces/<id>/' + (APP_PORT_DEFAULTS[t] || '') + '/v1.' }),
      (t === 'comfyui') && e(Alert, { type: 'info', showIcon: true, style: { marginBottom: 16 }, message: 'One-click app: just pick the image. ComfyUI starts automatically and opens at /spaces/<id>/' + APP_PORT_DEFAULTS.comfyui + '/.' }),
      (t === 'custom') && e(Alert, { type: 'info', showIcon: true, style: { marginBottom: 16 }, message: 'Custom: the image runs its own ENTRYPOINT/CMD. Serve a web UI on 8888 to get an Open button, and/or enable SSH below for shell access.' }),
      isAppType && e(Form.Item, { name: 'start_command', label: isApiType ? 'Serve Command' : 'Start Command', rules: isApiType ? [{ required: true, message: 'Serve command (with model) is required' }] : [], tooltip: 'Leave empty to use the framework default.' },
        e(Input.TextArea, { autoSize: { minRows: 1, maxRows: 4 }, placeholder: APP_START_DEFAULTS[t] || '' })),
      isAppType && e(Form.Item, { name: 'app_port', label: 'Port', style: { maxWidth: 180 }, tooltip: 'Port the process listens on.' },
        e(Select, { allowClear: true, placeholder: String(APP_PORT_DEFAULTS[t] || ''), options: PORT_OPTIONS })),
      needsGithub && e(Form.Item, { name: 'repo_url', label: 'GitHub Repo URL', tooltip: isNotebookType ? 'Optional. Leave empty for a blank workspace.' : 'Repo containing your app (cloned into the workspace).' },
        e(Input, { placeholder: isNotebookType ? 'optional, e.g. https://github.com/org/repo' : 'https://github.com/org/repo' })),
      needsGithub && e(Space, { style: { width: '100%' }, size: 16, align: 'start' },
        e(Form.Item, { name: 'branch', label: 'Branch', style: { width: 180 } }, e(Input, { placeholder: 'main' })),
        isNotebookType && e(Form.Item, { name: 'notebook_path', label: 'Notebook Path', style: { flex: 1, minWidth: 220 } }, e(Input, { placeholder: 'optional, e.g. notebooks/workshop.ipynb' }))
      ),
      e(Form.Item, { name: 'cover_url', label: 'Cover URL' }, e(Input, { placeholder: 'optional' })),
      e(Form.Item, { name: 'ssh_enabled', label: 'SSH Access (advanced)', valuePropName: 'checked', tooltip: 'Off by default. When on, instances launched from this template expose an SSH port (uses an extra NodePort). Login is key-only using each user\u2019s Profile SSH public key \u2014 no password.' },
        e(Switch, null)),
      canPublish && e(Form.Item, { name: 'enabled', label: 'Visibility', tooltip: 'Private = only you can see and launch it. Public = listed in the Gallery for everyone.' },
        e(Select, { options: [{ value: false, label: 'Private (only you)' }, { value: true, label: 'Public (Gallery)' }] }))
    );
  }

  window.TemplateFormFields = TemplateFormFields;
})();
