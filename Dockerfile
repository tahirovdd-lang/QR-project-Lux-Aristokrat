FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

RUN set -e && \
    echo 'Начинаем установку зависимостей из requirements.txt...' && \
    if [ ! -f requirements.txt ] || [ ! -s requirements.txt ]; then \
        echo 'WARNING: requirements.txt пуст или не существует'; \
    else \
        while IFS= read -r line || [ -n "$line" ]; do \
            [ -z "$line" ] && continue; \
            line=$(echo "$line" | tr -d '\r\0' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'); \
            [ -z "$line" ] && continue; \
            case "$line" in \
                \#*) continue ;; \
                *aiogram*) echo 'Пропускаем aiogram из requirements.txt - установим правильную версию отдельно' && continue ;; \
            esac; \
            echo "=== Устанавливаем: $line ==="; \
            if echo "$line" | grep -qiE '^(sqlite3|json|os|sys|time|datetime|re|random|math|logging|asyncio|collections|itertools|functools|operator|pathlib|urllib|http|socket|ssl|hashlib|base64|uuid|threading|multiprocessing|queue|concurrent|subprocess|shutil|tempfile|pickle|copy|weakref|gc|ctypes|struct|array|binascii|codecs|encodings|locale|gettext|argparse|configparser|csv|io|textwrap|string|unicodedata|readline|rlcompleter)$'; then \
                echo "ℹ️ Пропускаем встроенный модуль Python: $line"; \
                continue; \
            fi; \
            if ! pip install --no-cache-dir "$line"; then \
                echo "ERROR: Не удалось установить $line" && exit 1; \
            else \
                echo "✅ Успешно установлен: $line"; \
            fi; \
        done < requirements.txt; \
    fi && \
    echo 'Установка зависимостей из requirements.txt завершена'

RUN set -e && \
    echo 'Удаляем конфликтующие версии aiogram...' && \
    pip uninstall -y aiogram aiogram3 2>/dev/null || true && \
    echo 'Устанавливаем совместимую версию aiogram...' && \
    pip install --no-cache-dir aiogram==2.25.1 && \
    python -c "from aiogram.utils import executor; print('✅ Executor доступен')" && \
    echo '✅ Установлена совместимая версия aiogram: 2.25.1'

RUN echo '=== Проверка установленных пакетов ===' && \
    (pip list 2>/dev/null | grep -E '(aiogram|dotenv|telegram|requests|supabase)' || echo 'WARNING: Некоторые модули не найдены')

CMD ["python", "main.py"]
