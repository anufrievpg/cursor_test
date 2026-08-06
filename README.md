# cursor_test

Репозиторий для артефактов Cursor / ЭНКО.

## ОБИТИ на GitHub Pages

Сайт: **https://anufrievpg.github.io/cursor_test/**

Сейчас в настройках: **Deploy from a branch** → `main` → `/docs`.

Файлы уже в `main`:
- [`docs/index.html`](./docs/index.html) + [`docs/.nojekyll`](./docs/.nojekyll)
- запасной вариант: [`index.html`](./index.html) в корне (+ `.nojekyll`)

### Если Actions пишет *Canceling since a higher priority… @ main*

Это конфликт очереди встроенного `pages-build-deployment` (не контент файла). Сделайте вручную:

1. **Actions** → отмените все pending/queued `pages build and deployment`
2. **Settings → Pages** → Save ещё раз (или выключите/включите Pages)
3. Дождитесь **одного** успешного run на `main` (~1 мин), без новых пушей в это время

Кастомный Actions-workflow удалён — он конфликтовал с legacy Pages.
