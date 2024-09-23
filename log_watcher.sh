#!/bin/bash

# Проверяем, передан ли путь к файлу в качестве аргумента
if [ -z "$1" ]; then
  # Если нет, запрашиваем путь интерактивно
  read -p "Введите путь к файлу лога: " LOGFILE
else
  # Если путь передан, используем его
  LOGFILE="$1"
fi

# Проверка существования файла
if [ ! -f "$LOGFILE" ]; then
  echo "Файл не найден: $LOGFILE"
  exit 1
fi
# Количество строк для вывода
LINES=300

# Интервал обновления в секундах
INTERVAL=5

while true; do
  clear
  tail -n $LINES "$LOGFILE" | batcat --paging=never
  sleep $INTERVAL
done

