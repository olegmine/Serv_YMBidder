import json
from datetime import datetime
import argparse
from colorama import init, Fore, Style

init(autoreset=True)  # Инициализация colorama


def parse_log_line(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def format_log_entry(entry):
    timestamp = entry.get('timestamp', 'N/A')
    level = entry.get('level', 'N/A').upper()
    event = entry.get('event', 'N/A')
    logger = entry.get('logger', 'N/A')

    color = Fore.WHITE
    if level == 'ERROR':
        color = Fore.RED
    elif level == 'WARNING':
        color = Fore.YELLOW
    elif level == 'INFO':
        color = Fore.GREEN

    formatted = f"{color}[{timestamp}] {level:<7} {logger:<20} | {event}"

    # Добавляем дополнительные поля, если они есть
    for key, value in entry.items():
        if key not in ['timestamp', 'level', 'event', 'logger']:
            formatted += f"\n  {key}: {value}"

    return formatted + Style.RESET_ALL


def filter_logs(logs, level=None, start_date=None, end_date=None):
    filtered = logs
    if level:
        filtered = [log for log in filtered if log.get('level') == level.lower()]
    if start_date:
        filtered = [log for log in filtered if
                    datetime.strptime(log.get('timestamp', '1900-01-01'), "%Y-%m-%d %H:%M:%S %z") >= start_date]
    if end_date:
        filtered = [log for log in filtered if
                    datetime.strptime(log.get('timestamp', '2100-01-01'), "%Y-%m-%d %H:%M:%S %z") <= end_date]
    return filtered


def main(file_path, level=None, start_date=None, end_date=None):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            logs = [parse_log_line(line) for line in file if parse_log_line(line)]
    except UnicodeDecodeError:
        print("Ошибка при чтении файла с UTF-8 кодировкой. Попытка чтения с cp1251...")
        with open(file_path, 'r', encoding='cp1251') as file:
            logs = [parse_log_line(line) for line in file if parse_log_line(line)]

    filtered_logs = filter_logs(logs, level, start_date, end_date)

    for log in filtered_logs:
        print(format_log_entry(log))
        print('-' * 80)  # Разделитель между записями


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log viewer")
    parser.add_argument("file", help="Path to the log file")
    parser.add_argument("-l", "--level", help="Filter by log level (e.g., INFO, WARNING, ERROR)")
    parser.add_argument("-s", "--start", help="Start date for filtering (YYYY-MM-DD)")
    parser.add_argument("-e", "--end", help="End date for filtering (YYYY-MM-DD)")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d") if args.start else None
    end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else None

    main(args.file, args.level, start_date, end_date)