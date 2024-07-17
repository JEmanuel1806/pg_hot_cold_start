# Foobar

Foobar is a Python library for dealing with word pluralization.

## Nutzen

Anpassen auf deine Datenbank Parameter

```python
    # Database connection parameters
    db_params = {
        'dbname': 'imdb',
        'user': 'jan',
        'password': '',
        'host': 'localhost',  # e.g., 'localhost'
        'port': '5432'  # e.g., '5432'
    }
```

Das hier startet deine Datenbank im WSL über Python. Pfade musst du in ```subprocess.run``` auf deine anpassen.
Wenn das nicht geht, schau wie WSL deinen Server startet. Der Befehl muss dann in die "". Ansonsten frag ChatGPT wie er deinen Befehl darin ausführen würde.

```python
def start_postgres():
    try:
        # Start the PostgreSQL server on WSL
        subprocess.run(["wsl", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "-l", "logfile", "start"],
                       check=True)
        print("PostgreSQL server started on WSL.")

        # Optionally, you can wait for a few seconds to ensure the server has started
        time.sleep(5)
    except subprocess.CalledProcessError as e:
        print(f"Failed to start PostgreSQL server: {e}")


def stop_postgres():
    try:
        # Stop the PostgreSQL server on WSL
        subprocess.run(["wsl", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "stop"], check=True)
        print("PostgreSQL server stopped on WSL.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop PostgreSQL server: {e}")
    }
```

## Output

Immer zwei Diagramme, erst Execution Times und wenn Du das schließt dann (bisher) die Hits und Reads (noch nicht drin).


