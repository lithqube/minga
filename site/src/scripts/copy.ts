// Adds a copy button to every code block. Without JS there is no button and the code stays selectable.
const live = document.getElementById('live');
const say = (msg: string) => {
  if (live) live.textContent = msg;
};

/** Text to copy: an explicit data-copy, else the block's text minus "$ " prompts and output lines. */
function textOf(pre: HTMLElement): string {
  const host = pre.closest<HTMLElement>('[data-copy]');
  if (host?.dataset.copy) return host.dataset.copy;
  return pre.innerText.replace(/\n$/, '');
}

for (const pre of document.querySelectorAll<HTMLElement>('pre')) {
  if (pre.classList.contains('mermaid') || pre.closest('[data-nocopy]')) continue;
  const host = pre.parentElement?.classList.contains('code') ? pre.parentElement : null;
  const wrap = host ?? document.createElement('div');
  if (!host) {
    wrap.className = 'code';
    pre.replaceWith(wrap);
    wrap.append(pre);
  }
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'copy';
  btn.textContent = 'Copy';
  const first =
    textOf(pre)
      .split('\n')
      .find((l) => l.trim() && !l.trim().startsWith('#')) ?? '';
  btn.setAttribute('aria-label', `Copy code: ${first.slice(0, 60)}`);
  btn.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(textOf(pre));
      btn.textContent = 'Copied';
      say('Copied to clipboard');
    } catch {
      getSelection()?.selectAllChildren(pre);
      btn.textContent = 'Copy failed';
      say('Copy failed. The code is selected; press Ctrl+C or Cmd+C.');
    }
    setTimeout(() => (btn.textContent = 'Copy'), 2000);
  });
  wrap.classList.add('has-copy');
  wrap.append(btn);
}
