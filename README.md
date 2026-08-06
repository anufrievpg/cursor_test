# cursor_test

Репозиторий для артефактов Cursor / ЭНКО.

## ОБИТИ на GitHub Pages

<<<<<<< Updated upstream
После включения Pages страница будет доступна по адресу:

**https://anufrievpg.github.io/cursor_test/**

Прямой путь к файлу:  
https://anufrievpg.github.io/cursor_test/enco-obiti/enco-obiti-department.html

### Как включить (один раз)

1. Откройте **Settings → Pages** репозитория  
   https://github.com/anufrievpg/cursor_test/settings/pages
2. **Build and deployment → Source:** выберите **GitHub Actions**
3. Сохраните. Workflow `Deploy GitHub Pages` опубликует сайт с этой ветки / после merge в `main`.

Альтернатива без Actions: Source = **Deploy from a branch** → ветка `main` (или эта feature-ветка) → папка `/ (root)` или `/docs`.

> Сейчас у репозитория `has_pages: false`, а файл ОБИТИ жил только в PR. Без включения Pages и без `index.html` в корне публикации не было.
=======
Сайт: **https://anufrievpg.github.io/cursor_test/**

Сейчас в настройках: **Deploy from a branch** → `main` → `/docs`.

Файлы уже в `main`:
- [`docs/index.html`](./docs/index.html) + [`docs/.nojekyll`](./docs/.nojekyll)
- запасной вариант: [`index.html`](./index.html) в корне (+ `.nojekyll`)

### Если Actions пишет *Canceling since a higher priority… @ main*

Это конфликт очереди встроенного `pages-build-deployment` (не контент). Сделайте вручную:

1. **Actions** → отмените все pending/queued `pages build and deployment`
2. **Settings → Pages** → нажмите Save ещё раз (или выключите/включите Pages)
3. Дождитесь одного успешного run на `main` (~1 мин)

Кастомный Actions-workflow удалён — он как раз конфликтовал с legacy Pages.
>>>>>>> Stashed changes
