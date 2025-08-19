# server.py
from flask import Flask, jsonify, request, Response
from statsig import statsig
from statsig.statsig_event import StatsigEvent
from statsig.statsig_user import StatsigUser
import random
import os
import itertools
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# -------- Statsig initialization (non-fatal in dev) --------
API_KEY = os.environ.get("STATSIG_API_KEY")
STATSIG_ENABLED = False
if API_KEY:
    try:
        statsig.initialize(API_KEY)
        STATSIG_ENABLED = True
        logging.info("Statsig initialized.")
    except Exception as e:
        logging.warning("Statsig failed to initialize: %s", e)
else:
    logging.info("STATSIG_API_KEY not set — running with Statsig disabled (dev mode).")

# -------- In-memory storage (toy DB) --------
tasks = [
    {"id": 1, "title": "Do the dishes", "description": "Odd Tasks", "done": False},
    {"id": 2, "title": "Study for exam", "description": "Even Tasks", "done": False},
]
id_counter = itertools.count(len(tasks) + 1)

# Map experiment values to CSS colors (accept named or hex)
COLOR_MAP = {
    "Red": "#e74c3c",
    "Orange": "#e67e22",
    "Blue": "#3498db",
    "Green": "#2ecc71",
}


# -------- Helpers --------
def make_user():
    """
    Build a StatsigUser from the request.
    Behavior:
      - If `?random=1` query param is present, use a random id (dev).
      - Otherwise use remote_addr hashed to a string.
    """
    use_random = request.args.get("random")
    raw = str(random.randint(0, 1_000_000)) if use_random else request.remote_addr or "unknown"
    user_id = str(hash(raw))
    return StatsigUser(user_id)


def track_event(user: StatsigUser, event_name: str):
    """
    Log an event to Statsig. No-op if Statsig not enabled.
    Use a simple string event_name; keeps the StatsigEvent fields minimal and safe.
    """
    if not STATSIG_ENABLED:
        logging.debug("Statsig disabled — would have logged: %s for user %s", event_name, getattr(user, "user_id", None))
        return
    try:
        ev = StatsigEvent(user=user, event_name=event_name)
        statsig.log_event(ev)
    except Exception as e:
        logging.warning("Failed to log Statsig event %s: %s", event_name, e)


def get_experiment_for_user(user: StatsigUser):
    """
    Returns tuple (color_css, paragraph_text, raw_experiment_value)
    """
    if STATSIG_ENABLED:
        try:
            experiment = statsig.get_experiment(user, "button_color_v3")
        except Exception as e:
            logging.warning("Statsig get_experiment failed: %s", e)
            experiment = {}
    else:
        experiment = {}

    raw_color = experiment.get("Button Color", "Blue")
    paragraph_text = experiment.get("Paragraph Text", "Data Engineering Boot Camp")

    # Accept hex values or fallback to mapped color
    color_css = raw_color if (isinstance(raw_color, str) and raw_color.startswith("#")) else COLOR_MAP.get(raw_color, "#3498db")
    return color_css, paragraph_text, raw_color


# -------- Frontend routes (single-file UI) --------
@app.route("/")
def index():
    # Initial HTML is static; frontend JS will fetch /api/experiment and /api/tasks
    # This keeps HTML inside code, but makes it dynamic.
    return Response(
        """
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>Tasks — Experiments Demo</title>
          <meta name="viewport" content="width=device-width,initial-scale=1">
          <style>
            :root { --card-radius: 12px; --gap: 12px; }
            body { font-family: Inter, Roboto, Arial, sans-serif; background: #f5f7fa; margin: 0; padding: 2rem; color: #222; }
            .layout { max-width: 1000px; margin: 0 auto; }
            .header { display:flex; justify-content:space-between; gap: var(--gap); align-items:center; margin-bottom: 1rem; }
            .card { background: white; border-radius: var(--card-radius); padding: 1rem; box-shadow: 0 6px 20px rgba(10,10,10,0.06); }
            .experiment-banner { padding: 1rem; border-radius: 10px; color: white; }
            table { width:100%; border-collapse: collapse; margin-top: 1rem; }
            th, td { padding: 0.6rem 0.75rem; text-align:left; border-bottom: 1px solid #eee; }
            th { color: #444; font-weight: 600; }
            .controls { display:flex; gap:.5rem; align-items:center; }
            .btn { background:#2d9cdb; color: white; padding: .45rem .8rem; border-radius: 8px; text-decoration:none; cursor:pointer; border: none; }
            .btn.ghost { background: transparent; color: #2d9cdb; border: 1px solid #d7eaf7; }
            .small { font-size: .9rem; padding: .3rem .6rem; border-radius: 6px; }
            .form-row { display:flex; gap:.5rem; margin-top: .75rem; }
            input[type="text"] { flex:1; padding:.5rem; border-radius:8px; border:1px solid #ddd; }
            .muted { color: #666; font-size: .9rem; }
            .status { font-weight: 600; }
            .action { cursor: pointer; color: #2d9cdb; text-decoration: underline; }
          </style>
        </head>
        <body>
          <div class="layout">
            <div class="header">
              <div>
                <h1 style="margin:.1rem 0">Tasks & Experiments</h1>
                <div class="muted">Demo UI — experiments delivered by Statsig</div>
              </div>
              <div class="controls">
                <button id="refresh" class="btn small">Refresh</button>
                <button id="random" class="btn small ghost">Impersonate random user</button>
              </div>
            </div>

            <div id="experiment" class="card experiment-banner">
              Loading experiment...
            </div>

            <div class="card" style="margin-top:1rem">
              <div style="display:flex; justify-content:space-between; align-items:center;">
                <div><strong>Tasks</strong> <span class="muted" id="task-count"></span></div>
                <div><span class="muted">User ID: </span><span id="user-id" class="muted"></span></div>
              </div>

              <table id="tasks-table" aria-live="polite">
                <thead>
                  <tr><th>Id</th><th>Title</th><th>Description</th><th>Done</th><th>Actions</th></tr>
                </thead>
                <tbody></tbody>
              </table>

              <div class="form-row">
                <input id="title" placeholder="New task title" type="text" />
                <input id="desc" placeholder="Description (optional)" type="text" />
                <button id="create" class="btn">Create</button>
              </div>
            </div>
          </div>

          <script>
            const qs = new URLSearchParams(location.search);
            let useRandom = qs.has('random');
            const $ = sel => document.querySelector(sel);
            const tbody = document.querySelector('#tasks-table tbody');
            const taskCount = $('#task-count');
            const userIdEl = $('#user-id');
            const experimentBanner = $('#experiment');

            async function api(path, opts) {
              const url = path + (useRandom ? '?random=1' : '');
              const res = await fetch(url, Object.assign({ headers: { 'Content-Type': 'application/json' } }, opts));
              if (!res.ok) {
                const text = await res.text();
                console.warn('API error', res.status, text);
                throw new Error('API error ' + res.status);
              }
              return res.json().catch(() => null);
            }

            function renderTasks(tasks) {
              tbody.innerHTML = '';
              for (const t of tasks) {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                  <td>${t.id}</td>
                  <td>${escapeHtml(t.title)}</td>
                  <td>${escapeHtml(t.description || '')}</td>
                  <td>${t.done ? '✅' : '❌'}</td>
                  <td>
                    <button data-id="${t.id}" class="btn small toggle">${t.done ? 'Mark undone' : 'Mark done'}</button>
                    <button data-id="${t.id}" class="btn small ghost delete">Delete</button>
                  </td>
                `;
                tbody.appendChild(tr);
              }
              taskCount.textContent = '(' + tasks.length + ' items)';
            }

            function escapeHtml(s){ return s ? s.replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;') : ''; }

            async function refreshAll() {
              try {
                const exp = await api('/api/experiment');
                const tasks = await api('/api/tasks');
                userIdEl.textContent = exp.user_id || '';
                experimentBanner.style.background = exp.color;
                experimentBanner.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;">
                  <div><strong>Experiment:</strong> ${escapeHtml(exp.raw)}</div>
                  <div class="muted">${escapeHtml(exp.paragraph)}</div>
                </div>`;
                renderTasks(tasks);
                attachActions();
              } catch (e) {
                console.error(e);
                experimentBanner.textContent = 'Failed to load experiment / tasks (see console).';
              }
            }

            function attachActions(){
              document.querySelectorAll('.toggle').forEach(b=>{
                b.onclick = async (ev)=>{
                  const id = b.getAttribute('data-id');
                  try {
                    // optimistic UI: flip locally then sync
                    b.disabled = true;
                    await api('/api/tasks/' + id, {
                      method: 'PUT',
                      body: JSON.stringify({ done: !b.closest('tr').querySelector('td:nth-child(4)').innerText.includes('✅') })
                    });
                    await refreshAll();
                  } catch(e) {
                    console.error(e);
                    b.disabled = false;
                  }
                };
              });
              document.querySelectorAll('.delete').forEach(b=>{
                b.onclick = async (ev)=>{
                  const id = b.getAttribute('data-id');
                  if (!confirm('Delete task ' + id + '?')) return;
                  try {
                    await api('/api/tasks/' + id, { method: 'DELETE' });
                    await refreshAll();
                  } catch(e) { console.error(e); }
                };
              });
            }

            $('#create').onclick = async ()=>{
              const title = $('#title').value.trim();
              const desc = $('#desc').value.trim();
              if (!title) { alert('Title required'); return; }
              try {
                await api('/api/tasks', { method: 'POST', body: JSON.stringify({ title, description: desc }) });
                $('#title').value = '';
                $('#desc').value = '';
                await refreshAll();
              } catch(e) { console.error(e); alert('Create failed'); }
            };

            $('#refresh').onclick = refreshAll;
            $('#random').onclick = ()=>{
              useRandom = !useRandom;
              if (useRandom) {
                qs.set('random', '1');
                history.replaceState(null, '', '?' + qs.toString());
                $('#random').textContent = 'Stop impersonating';
              } else {
                qs.delete('random');
                history.replaceState(null, '', location.pathname);
                $('#random').textContent = 'Impersonate random user';
              }
              refreshAll();
            };

            // initial load
            refreshAll();
          </script>
        </body>
        </html>
        """,
        mimetype="text/html",
    )


# -------- API: experiment metadata (so frontend can render dynamically) --------
@app.route("/api/experiment", methods=["GET"])
def api_experiment():
    user = make_user()
    color_css, paragraph_text, raw = get_experiment_for_user(user)
    # Log that the UI was viewed
    track_event(user, "view_tasks_page")
    return jsonify({"color": color_css, "paragraph": paragraph_text, "raw": raw, "user_id": user.user_id})


# -------- API: tasks CRUD (log actions for analytics) --------
@app.route("/api/tasks", methods=["GET"])
def api_get_tasks():
    # Return the whole list for this demo
    return jsonify(tasks)


@app.route("/api/tasks/<int:task_id>", methods=["GET"])
def api_get_task(task_id):
    t = next((x for x in tasks if x["id"] == task_id), None)
    if not t:
        return jsonify({"error": "not found"}), 404
    return jsonify(t)


@app.route("/api/tasks", methods=["POST"])
def api_create_task():
    data = request.get_json(silent=True) or {}
    title = data.get("title")
    if not title:
        return jsonify({"error": "title required"}), 400
    task = {
        "id": next(id_counter),
        "title": title,
        "description": data.get("description", ""),
        "done": False,
    }
    tasks.append(task)
    user = make_user()
    track_event(user, f"created_task:{task['id']}")
    return jsonify(task), 201


@app.route("/api/tasks/<int:task_id>", methods=["PUT"])
def api_update_task(task_id):
    t = next((x for x in tasks if x["id"] == task_id), None)
    if not t:
        return jsonify({"error": "not found"}), 404
    data = request.get_json(silent=True) or {}
    # accept partial updates
    if "title" in data:
        t["title"] = data["title"]
    if "description" in data:
        t["description"] = data["description"]
    if "done" in data:
        t["done"] = bool(data["done"])
    user = make_user()
    track_event(user, f"updated_task:{task_id}")
    return jsonify(t)


@app.route("/api/tasks/<int:task_id>", methods=["DELETE"])
def api_delete_task(task_id):
    global tasks
    existed = any(x for x in tasks if x["id"] == task_id)
    tasks = [x for x in tasks if x["id"] != task_id]
    user = make_user()
    if existed:
        track_event(user, f"deleted_task:{task_id}")
        return jsonify({"result": True})
    else:
        return jsonify({"error": "not found"}), 404


# -------- Run (dev) --------
if __name__ == "__main__":
    app.run(debug=True)
