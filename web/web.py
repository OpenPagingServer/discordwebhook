import html
from urllib.parse import urlparse


ENDPOINT_TABLE = "endpoints-output-discord"
SETTINGS_TABLE = "endpoints-modulesettings-discord"
DEFAULT_SETTINGS = {
    "username": "",
    "avatar-url": "",
    "tts": "0",
    "use-embeds": "1",
}


def h(value):
    return html.escape("" if value is None else str(value), quote=True)


def truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def forms():
    return {
        "webhook": {
            "label": "Discord Webhook",
            "description": "Send Open Paging Server alerts to a Discord webhook endpoint.",
        },
    }


def ensure_schema(conn_factory):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{ENDPOINT_TABLE}` ("
                "`id` INT NOT NULL AUTO_INCREMENT, "
                "`name` VARCHAR(255) NOT NULL DEFAULT '', "
                "`webhook_url` VARCHAR(2048) NOT NULL DEFAULT '', "
                "`status` VARCHAR(32) NOT NULL DEFAULT 'Unchecked', "
                "`mention_text` VARCHAR(255) NOT NULL DEFAULT '', "
                "`username` VARCHAR(80) NOT NULL DEFAULT '', "
                "`avatar_url` VARCHAR(2048) NOT NULL DEFAULT '', "
                "`exclude_bells` TINYINT(1) NOT NULL DEFAULT 1, "
                "PRIMARY KEY (`id`), KEY `status_idx` (`status`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{SETTINGS_TABLE}` ("
                "`parameter` VARCHAR(128) NOT NULL, `value` TEXT, PRIMARY KEY (`parameter`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            )
            cur.execute(f"SHOW COLUMNS FROM `{ENDPOINT_TABLE}`")
            endpoint_columns = {row["Field"] for row in cur.fetchall()}
            if "exclude_bells" not in endpoint_columns:
                cur.execute(
                    f"ALTER TABLE `{ENDPOINT_TABLE}` "
                    "ADD COLUMN `exclude_bells` TINYINT(1) NOT NULL DEFAULT 1"
                )
            for key, value in DEFAULT_SETTINGS.items():
                cur.execute(
                    f"INSERT INTO `{SETTINGS_TABLE}` (`parameter`, `value`) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE `parameter` = `parameter`",
                    (key, value),
                )
        conn.commit()
    finally:
        conn.close()


def query_all(conn_factory, sql, params=()):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def query_one(conn_factory, sql, params=()):
    rows = query_all(conn_factory, sql, params)
    return rows[0] if rows else None


def execute(conn_factory, sql, params=()):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def module_body(content):
    return (
        "<style>body{font-family:Tahoma,sans-serif;margin:0;padding:18px;color:#202124;background:#fff}.grid{display:grid;gap:12px}.row{display:grid;gap:6px}"
        "label{font-weight:500}.check{display:flex;align-items:center;gap:8px;font-weight:400}.control{padding:10px;border:1px solid #ddd;border-radius:4px;font:inherit;box-sizing:border-box;width:100%}"
        ".button,button{background:#5865F2;color:#fff;border:0;border-radius:4px;padding:10px 14px;font:inherit;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center}"
        ".button.secondary{background:#5f6368}.danger{background:#c62828}.success{background:#e8f5e9;border:1px solid #a5d6a7;color:#1b5e20;padding:10px;border-radius:6px;margin-bottom:12px}"
        ".error{background:#ffebee;border:1px solid #ef9a9a;color:#b71c1c;padding:10px;border-radius:6px;margin-bottom:12px}.warn{background:#fff8e1;border:1px solid #ffe082;color:#5d4037;padding:12px;border-radius:6px;margin-bottom:12px}"
        ".note,.meta{color:#5f6368}.section{border-top:1px solid #eee;padding-top:12px;margin-top:4px}.hidden{display:none!important}"
        "@media(prefers-color-scheme:dark){body{background:#1e1e1e;color:#e0e0e0}.control{background:#171717;border-color:#333;color:#eee}.button,button{background:#7983F5;color:#fff}.button.secondary{background:#444;color:#eee}.note,.meta{color:#aaa}}</style>"
        + content
    )


def alert(message, error):
    out = ""
    if message:
        out += f'<div class="success">{h(message)}</div>'
    if error:
        out += f'<div class="error">{h(error)}</div>'
    return out


def looks_like_webhook_url(value):
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) >= 4 and parts[-3] == "webhooks"


def render_form(form_type, request, conn_factory, page, user):
    ensure_schema(conn_factory)
    if form_type not in forms():
        return page("Endpoint Form", module_body("<h1>Endpoint form not found</h1>"), "endpoints", user, status=404)
    message = ""
    error = ""
    values = {
        "name": "",
        "webhook_url": "",
        "mention_text": "",
        "username": "",
        "avatar_url": "",
        "unchecked": "",
        "exclude_bells": "1",
    }
    if request.method == "POST":
        try:
            for key in values:
                values[key] = str(request.form.get(key, values[key]) or "").strip()
            if not values["webhook_url"]:
                raise ValueError("Webhook URL is required.")
            if not looks_like_webhook_url(values["webhook_url"]):
                raise ValueError("Webhook URL must be a Discord webhook URL.")
            if query_one(conn_factory, f"SELECT `id` FROM `{ENDPOINT_TABLE}` WHERE `webhook_url`=%s", (values["webhook_url"],)):
                raise ValueError("That Discord webhook is already configured.")
            status = "Unchecked" if request.form.get("unchecked") else "New"
            exclude_bells = 0 if request.form.get("allow_bells") else 1
            execute(
                conn_factory,
                f"INSERT INTO `{ENDPOINT_TABLE}` (`name`, `webhook_url`, `status`, `mention_text`, `username`, `avatar_url`, `exclude_bells`) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    values["name"],
                    values["webhook_url"],
                    status,
                    values["mention_text"],
                    values["username"],
                    values["avatar_url"],
                    exclude_bells,
                ),
            )
            message = "Discord webhook endpoint added."
            values = {key: "" for key in values}
            values["exclude_bells"] = "1"
        except Exception as exc:
            error = str(exc)
    allow_bells_checked = "" if truthy(values.get("exclude_bells")) else " checked"
    body = (
        f"{alert(message, error)}<form method='post' class='grid'>"
        f"<div class='row'><label>Name</label><input class='control' name='name' value='{h(values['name'])}' placeholder='Dispatch Channel'></div>"
        f"<div class='row'><label>Webhook URL</label><input class='control' name='webhook_url' value='{h(values['webhook_url'])}' placeholder='https://discord.com/api/webhooks/...' required></div>"
        f"<div class='row'><label>Mention Text</label><input class='control' name='mention_text' value='{h(values['mention_text'])}' placeholder='@everyone or &lt;@&amp;1234567890&gt;'></div>"
        f"<div class='row'><label>Username Override</label><input class='control' name='username' value='{h(values['username'])}' placeholder='OPS Alerts'></div>"
        f"<div class='row'><label>Avatar URL Override</label><input class='control' type='url' name='avatar_url' value='{h(values['avatar_url'])}' placeholder='https://example.com/avatar.png'></div>"
        f"<label class='check'><input type='checkbox' name='allow_bells' value='1'{allow_bells_checked}> Allow bells on this endpoint</label>"
        "<div class='note'>Bells are excluded by default on new Discord webhook endpoints.</div>"
        f"<label class='check'><input type='checkbox' name='unchecked' value='1'{' checked' if values.get('unchecked') else ''}> Do not check webhook status in the background loop</label>"
        "<button class='button' type='submit'>Add Discord Webhook Endpoint</button></form>"
    )
    return page(forms()[form_type]["label"], module_body(body), "endpoints", user)


def render_action(action, endpoint_id, request, conn_factory, page, user):
    ensure_schema(conn_factory)
    message = ""
    error = ""
    row = None
    try:
        row_id = int(str(endpoint_id or "").strip())
        row = query_one(
            conn_factory,
            f"SELECT `id`, `name`, `webhook_url`, `status`, `mention_text`, `username`, `avatar_url`, `exclude_bells` "
            f"FROM `{ENDPOINT_TABLE}` WHERE `id`=%s",
            (row_id,),
        )
        if not row:
            raise ValueError("Endpoint not found.")

        if request.method == "POST":
            if action == "delete":
                execute(conn_factory, f"DELETE FROM `{ENDPOINT_TABLE}` WHERE `id`=%s", (row["id"],))
                return page("Endpoint Deleted", module_body("<script>window.top.location.href='/admin/manage-endpoints'</script><div class='success'>Discord endpoint deleted.</div>"), "endpoints", user)

            values = {
                "name": str(request.form.get("name", "") or "").strip(),
                "webhook_url": str(request.form.get("webhook_url", "") or "").strip(),
                "mention_text": str(request.form.get("mention_text", "") or "").strip(),
                "username": str(request.form.get("username", "") or "").strip(),
                "avatar_url": str(request.form.get("avatar_url", "") or "").strip(),
            }
            if not values["webhook_url"]:
                raise ValueError("Webhook URL is required.")
            if not looks_like_webhook_url(values["webhook_url"]):
                raise ValueError("Webhook URL must be a Discord webhook URL.")
            duplicate = query_one(
                conn_factory,
                f"SELECT `id` FROM `{ENDPOINT_TABLE}` WHERE `webhook_url`=%s AND `id`<>%s",
                (values["webhook_url"], row["id"]),
            )
            if duplicate:
                raise ValueError("That Discord webhook is already configured.")
            status = "Unchecked" if request.form.get("unchecked") else "New"
            exclude_bells = 0 if request.form.get("allow_bells") else 1
            execute(
                conn_factory,
                f"UPDATE `{ENDPOINT_TABLE}` SET `name`=%s, `webhook_url`=%s, `status`=%s, `mention_text`=%s, `username`=%s, `avatar_url`=%s, `exclude_bells`=%s WHERE `id`=%s",
                (
                    values["name"],
                    values["webhook_url"],
                    status,
                    values["mention_text"],
                    values["username"],
                    values["avatar_url"],
                    exclude_bells,
                    row["id"],
                ),
            )
            return page("Endpoint Saved", module_body("<script>window.top.location.href='/admin/manage-endpoints'</script><div class='success'>Discord endpoint updated.</div>"), "endpoints", user)
    except Exception as exc:
        error = str(exc)

    if action == "delete":
        body = alert(message, error)
        if row:
            label = row.get("name") or f"Discord Webhook {row.get('id')}"
            body += f"<div class='warn'>Delete {h(label)}?</div><form method='post'><button class='button danger' type='submit'>Delete Endpoint</button></form>"
        return page("Delete Discord Endpoint", module_body(body), "endpoints", user)

    if not row:
        return page("Edit Discord Endpoint", module_body(alert(message, error)), "endpoints", user)

    allow_bells_checked = "" if truthy(row.get("exclude_bells")) else " checked"
    body = (
        f"{alert(message, error)}<p class='meta'>Current status: {h(row.get('status'))}</p><form method='post' class='grid'>"
        f"<div class='row'><label>Name</label><input class='control' name='name' value='{h(row.get('name'))}'></div>"
        f"<div class='row'><label>Webhook URL</label><input class='control' name='webhook_url' value='{h(row.get('webhook_url'))}' required></div>"
        f"<div class='row'><label>Mention Text</label><input class='control' name='mention_text' value='{h(row.get('mention_text'))}'></div>"
        f"<div class='row'><label>Username Override</label><input class='control' name='username' value='{h(row.get('username'))}'></div>"
        f"<div class='row'><label>Avatar URL Override</label><input class='control' type='url' name='avatar_url' value='{h(row.get('avatar_url'))}'></div>"
        f"<label class='check'><input type='checkbox' name='allow_bells' value='1'{allow_bells_checked}> Allow bells on this endpoint</label>"
        f"<label class='check'><input type='checkbox' name='unchecked' value='1'{' checked' if row.get('status') == 'Unchecked' else ''}> Do not check webhook status in the background loop</label>"
        "<button class='button'>Save Discord Endpoint</button></form>"
    )
    return page("Edit Discord Endpoint", module_body(body), "endpoints", user)


def load_settings(conn_factory):
    ensure_schema(conn_factory)
    values = dict(DEFAULT_SETTINGS)
    for row in query_all(conn_factory, f"SELECT `parameter`, `value` FROM `{SETTINGS_TABLE}`"):
        key = str(row.get("parameter") or "")
        if key in values:
            values[key] = "" if row.get("value") is None else str(row.get("value"))
    save_settings(conn_factory, values)
    return values


def save_settings(conn_factory, values):
    conn = conn_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{SETTINGS_TABLE}`")
            for key, value in values.items():
                cur.execute(
                    f"INSERT INTO `{SETTINGS_TABLE}` (`parameter`, `value`) VALUES (%s, %s)",
                    (key, value),
                )
        conn.commit()
    finally:
        conn.close()


def render_settings(request, conn_factory, page, user):
    values = load_settings(conn_factory)
    message = ""
    error = ""
    if request.method == "POST":
        try:
            values = {
                "username": str(request.form.get("username", "") or "").strip(),
                "avatar-url": str(request.form.get("avatar-url", "") or "").strip(),
                "tts": "1" if request.form.get("tts") else "0",
                "use-embeds": "1" if request.form.get("use-embeds") else "0",
            }
            save_settings(conn_factory, values)
            message = "Discord module settings saved."
        except Exception as exc:
            error = str(exc)
    checked = lambda key: " checked" if truthy(values.get(key)) else ""
    body = f"""
{alert(message, error)}
<form method="post" class="grid">
    <div class="row">
        <label>Default Username</label>
        <input class="control" name="username" value="{h(values.get("username"))}" placeholder="OPS Alerts">
    </div>

    <div class="row">
        <label>Default Avatar URL</label>
        <input class="control" type="url" name="avatar-url" value="{h(values.get("avatar-url"))}" placeholder="https://example.com/avatar.png">
    </div>

    <div class="section grid">
        <label class="check"><input type="checkbox" name="use-embeds" value="1"{checked("use-embeds")}> Send alerts as Discord embeds</label>
        <label class="check"><input type="checkbox" name="tts" value="1"{checked("tts")}> Enable Discord TTS</label>
    </div>

    <button class="button" type="submit">Save Discord Settings</button>
</form>"""
    return page("DiscordWebhook Settings", module_body(body), "endpoints", user)
