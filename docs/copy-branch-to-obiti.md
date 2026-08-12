# Копирование ветки в anufrievpg/OBITI

Источник: `anufrievpg/cursor_test` → ветка `cursor/obiti-department-canvas-00af`  
Назначение: `anufrievpg/OBITI` → та же ветка

## Вариант 1 — локально (одна команда)

```bash
git push https://github.com/anufrievpg/OBITI.git \
  cursor/obiti-department-canvas-00af:cursor/obiti-department-canvas-00af
```

Выполните из клона `cursor_test`, будучи авторизованным в GitHub как владелец `OBITI`.

## Вариант 2 — GitHub Actions

1. Создайте PAT (classic) с правом `repo`.
2. Добавьте секрет `OBITI_PUSH_TOKEN` в  
   https://github.com/anufrievpg/cursor_test/settings/secrets/actions
3. Запустите workflow **Copy branch to OBITI** в Actions.

## Вариант 3 — доступ Cursor GitHub App

В настройках GitHub App Cursor добавьте репозиторий `anufrievpg/OBITI` в список доступных — тогда Cloud Agent сможет пушить напрямую.
