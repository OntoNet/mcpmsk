# Onto MCP Server - Настройка и использование

## ✅ Статус сервера
**MCP сервер готов к работе!** 🚀

### 🔧 Доступные MCP Tools:
- ✅ `login_with_credentials(email, password)` - авторизация
- ✅ `get_auth_status()` - статус авторизации  
- ✅ `get_session_info()` - информация о сессии
- ✅ `refresh_token()` - обновление токена
- ✅ `logout()` - выход из системы
- ✅ `search_templates(name_part, realm_id, include_children, include_parents)` - поиск шаблонов
- ✅ `search_objects(realm_id, name_filter, template_uuid, comment_filter, load_all, page_size)` - поиск объектов
- ✅ `list_available_realms()` - список доступных реалмов

## 🚀 Запуск сервера

### Метод 1: Прямой запуск
```bash
cd <путь_к_проекту>
python -m onto_mcp.server
```

### Метод 2: HTTP режим
```bash
cd <путь_к_проекту>
set MCP_TRANSPORT=http
set PORT=8080
python -m onto_mcp.server
```

## 🔗 Подключение из других приложений

### Cursor/VS Code/Claude Desktop
Добавьте в файл `mcp.json`:

```json
{
  "mcpServers": {
    "onto-mcp-server": {
      "command": "python",
      "args": ["-m", "onto_mcp.server"],
      "cwd": "/путь/к/проекту",
      "env": {
        "KEYCLOAK_BASE_URL": "https://app.ontonet.ru",
        "KEYCLOAK_REALM": "onto",
        "KEYCLOAK_CLIENT_ID": "frontend-prod",
        "ONTO_API_BASE": "https://app.ontonet.ru/api/v2/core"
      }
    }
  }
}
```

### HTTP клиенты
```bash
POST http://localhost:8080/tools/login_with_credentials
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password"
}
```

## 🔐 Авторизация

### Первый запуск:
```python
# В любом MCP клиенте
login_with_credentials("av2@ontonet.ru", "av233")
```

### Проверка статуса:
```python
get_auth_status()
```

## 🔍 Использование поиска

### Список реалмов:
```python
list_available_realms()
```

### Поиск шаблонов:
```python
# Простой поиск
search_templates("AV22")

# В конкретном реалме
search_templates("AV22", realm_id="9c288f96-3e98-44fb-a4a3-50ab0ed28c58")

# С включением дочерних элементов
search_templates("модель", include_children=True, include_parents=True)
```

### 🆕 Поиск объектов (с пагинацией):
```python
# Поиск по имени (первая страница)
search_objects(name_filter="кот")

# Поиск по шаблону
search_objects(template_uuid="template-uuid-here")

# Поиск по комментарию
search_objects(comment_filter="описание")

# Получить ВСЕ объекты определенного шаблона (полный датасет)
search_objects(template_uuid="template-uuid", load_all=True)

# Комбинированный поиск в конкретном реалме
search_objects(
    realm_id="realm-id-here",
    name_filter="модель",
    template_uuid="template-uuid", 
    load_all=True,
    page_size=50
)
```

## 📊 Текущая авторизация
- **Пользователь:** av2@ontonet.ru ✅
- **Статус:** Authenticated ✅
- **Доступные реалмы:** 3 ✅
  - av2@ontonet.ru personal realm
  - zxcxzcxzc
  - *** Примеры моделей ***

## 🛠️ Разработка
Все dev-скрипты находятся в `dev-scripts/` и игнорируются git.

## 🔧 Решение проблем

### ⚠️ "Сервер не отвечает / зависает"
**Проблема:** MCP клиент не получает ответы от сервера.

**Причина:** Отладочные сообщения попадали в stdout, нарушая MCP протокол.

**Решение:** ✅ **ИСПРАВЛЕНО** - все отладочные сообщения теперь идут в stderr.

**Производительность:**
- Инициализация: ~1.7с
- get_auth_status: ~0.8с  
- list_available_realms: ~0.9с
- search_templates: ~1.0с

### 🌐 Проблемы с сетью
Если операции занимают > 10 сек:
1. Проверьте подключение к `app.ontonet.ru`
2. Проверьте срок действия токенов
3. Обновите токены через `refresh_token()`

## 📝 Примеры использования
```python
# 1. Авторизация
login_with_credentials("email@ontonet.ru", "password")

# 2. Поиск шаблона "AV22 Коты"
search_templates("AV22", realm_id="9c288f96-3e98-44fb-a4a3-50ab0ed28c58")

# 3. Поиск объектов по имени
search_objects(name_filter="кот", load_all=True)

# 4. Поиск всех объектов конкретного шаблона
search_objects(template_uuid="template-uuid-here", load_all=True)

# 5. Список всех реалмов
list_available_realms()

# 6. Выход
logout()
```

**Сервер готов к использованию! 🎉** 