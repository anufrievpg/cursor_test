# cursor_test

Публичный портал через **GitHub Pages**:  
https://anufrievpg.github.io/cursor_test/

## Как выложить файл

1. Положите HTML (или другие статические файлы) в папку **`docs/`**.
   - Пример: `docs/my-report.html`
   - Можно в подпапке: `docs/enco-obiti/page.html`
2. Закоммитьте и смержите изменения в ветку **`main`** (через PR или напрямую).
3. GitHub Actions задеплоит содержимое `docs/` на Pages.
4. Откройте файл по адресу:
   - `https://anufrievpg.github.io/cursor_test/my-report.html`
   - или `https://anufrievpg.github.io/cursor_test/enco-obiti/page.html`

По желанию добавьте ссылку на файл в [`docs/index.html`](docs/index.html), чтобы он отображался на главной портала.

## Первый запуск Pages (один раз)

Если сайт ещё не открывается:

1. Репозиторий → **Settings** → **Pages**
2. **Source**: выберите **GitHub Actions**
3. Дождитесь успешного workflow **Deploy GitHub Pages** (вкладка **Actions**)
4. Либо запустите workflow вручную: **Actions** → **Deploy GitHub Pages** → **Run workflow**

После этого портал доступен по ссылке выше. Для приватного репозитория нужен GitHub Pro / Team / Enterprise — сейчас репозиторий публичный, Pages работает бесплатно.

## Структура

```
docs/                         ← корень сайта Pages
  index.html                  ← главная портала
.github/workflows/
  deploy-pages.yml            ← автодеплой при push в main
```
