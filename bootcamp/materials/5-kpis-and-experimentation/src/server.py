# server.py — minimal & delightful with full logic
from flask import Flask, jsonify, request, Response
import os, random, itertools, hashlib, logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Optional Statsig
STATSIG_ENABLED = False
try:
    API_KEY = os.environ.get("STATSIG_API_KEY")
    if API_KEY:
        from statsig import statsig
        from statsig.statsig_user import StatsigUser
        statsig.initialize(API_KEY)
        STATSIG_ENABLED = True
        logging.info("Statsig enabled.")
except Exception as e:
    logging.warning("Statsig not active: %s", e)

# In-memory tasks
tasks = [
    {"id": 1, "title": "Do the dishes", "description": "Odd Tasks", "done": False},
    {"id": 2, "title": "Study for exam", "description": "Even Tasks", "done": False},
]
id_counter = itertools.count(len(tasks) + 1)

DEMO_PALETTE = ["#e74c3c", "#e67e22", "#3498db", "#2ecc71", "#9b59b6", "#f1c40f"]
COLOR_MAP = {"red": "#e74c3c", "orange": "#e67e22", "blue": "#3498db", "green": "#2ecc71"}

def deterministic_color_for_user_id(user_id: str) -> str:
    if not user_id:
        return DEMO_PALETTE[0]
    h = hashlib.sha1(user_id.encode()).hexdigest()
    return DEMO_PALETTE[int(h[:8], 16) % len(DEMO_PALETTE)]

def make_user_obj():
    seed = request.args.get("seed")
    use_random = bool(request.args.get("random"))

    raw = seed if seed else (request.remote_addr or "unknown")
    user_id = str(hash(raw))
    statsig_user = None
    if STATSIG_ENABLED:
        from statsig.statsig_user import StatsigUser
        statsig_user = StatsigUser(user_id)
    return user_id, statsig_user


def get_experiment_for_user(user_id: str, statsig_user, force_demo: bool):
    if force_demo:
        return deterministic_color_for_user_id(user_id), "Demo (impersonated)", f"demo:{user_id}"

    if STATSIG_ENABLED and statsig_user is not None:
        try:
            exp = statsig.get_experiment(statsig_user, "button_color_v3") or {}
            raw_color = exp.get("Button Color") or exp.get("button_color") or "Blue"
            paragraph = exp.get("Paragraph Text") or exp.get("paragraph_text") or "Data Engineering Boot Camp"
            if isinstance(raw_color, str) and raw_color.strip().startswith("#"):
                return raw_color.strip(), paragraph, raw_color
            return COLOR_MAP.get(raw_color.strip().lower(), "#3498db"), paragraph, raw_color
        except Exception as e:
            logging.warning("Statsig experiment fetch failed: %s", e)

    return "#3498db", "Data Engineering Boot Camp", "default:Blue"

# Frontend
@app.route("/")
def index():
    return Response(
"""
<!doctype html>
<html><head>
<meta charset="utf-8"><title>Tasks demo</title>
<style>
body{font-family:Arial;background:#f5f7fa;margin:2rem}
.card{background:#fff;padding:1rem;border-radius:8px;box-shadow:0 6px 18px rgba(0,0,0,.06)}
table{width:100%;margin-top:1rem;border-collapse:collapse}
th,td{padding:.5rem;border-bottom:1px solid #eee;text-align:left}
.controls{display:flex;gap:.5rem}
#badge{position:fixed;top:10px;right:10px;background:#9333ea;color:white;padding:6px 12px;border-radius:999px;cursor:pointer;transition:0.2s;}
</style>
</head><body>
<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <h2 style="margin:.2rem">Tasks & Experiments</h2>
    <div class="controls">
      <button id="refresh">Refresh</button>
      <button id="random">Impersonate random user</button>
    </div>
  </div>
  <div id="banner" style="margin-top:.75rem;padding:.75rem;border-radius:8px;color:white;background:#3498db">Loading...</div>

  <div style="margin-top:.75rem">
    <table>
      <thead><tr><th>Id</th><th>Title</th><th>Description</th><th>Done</th><th>Actions</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
    <div style="margin-top:.75rem;display:flex;gap:.5rem">
      <input id="title" placeholder="title" />
      <input id="desc" placeholder="description" />
      <button id="create">Create</button>
    </div>
  </div>
</div>

<div id="badge">🎭 <span id="badge-text"></span></div>

<script>
let useRandom=false, impersonationSeed=null;
function newSeed(){return Date.now().toString(36)+'-'+Math.floor(Math.random()*1e9).toString(36);}
const el=id=>document.getElementById(id);
const api=async(path,opts=undefined)=>{const url=new URL(path,location.href);if(useRandom){url.searchParams.set('random','1');if(impersonationSeed)url.searchParams.set('seed',impersonationSeed);}const res=await fetch(url.toString(),Object.assign({headers:{'Content-Type':'application/json'}},opts));if(!res.ok){const t=await res.text();console.warn('API error',res.status,t);throw new Error('API error '+res.status);}return res.json().catch(()=>null);};

async function refreshBadge(user){const el=document.getElementById('badge-text');el.textContent=user;const badge=document.getElementById('badge');badge.style.transform='scale(1.1)';setTimeout(()=>badge.style.transform='',200);}

async function refresh(){try{
  const exp=await api('/api/experiment');
  const tasks=await api('/api/tasks');
  el('banner').style.background=exp.color;
  el('banner').innerText=exp.raw+' — '+exp.paragraph;
  el('tbody').innerHTML=tasks.map(t=>`<tr><td>${t.id}</td><td>${t.title}</td><td>${t.description||''}</td><td>${t.done?'✅':'❌'}</td><td><button onclick="toggle(${t.id})">toggle</button> <button onclick="del(${t.id})">del</button></td></tr>`).join('');
  if(useRandom) refreshBadge(exp.user_id);
}catch(e){console.error('refresh failed',e);}}

async function toggle(id){const row=Array.from(document.querySelectorAll('#tbody tr')).find(r=>r.innerText.startsWith(id.toString()));if(!row)return;const done=row.cells[3].innerText.includes('✅');await api('/api/tasks/'+id,{method:'PUT',body:JSON.stringify({done:!done})});refresh();}
async function del(id){if(!confirm('Delete?'))return;await api('/api/tasks/'+id,{method:'DELETE'});refresh();}
el('create').onclick=async()=>{const t=el('title').value.trim();if(!t)return alert('title required');await api('/api/tasks',{method:'POST',body:JSON.stringify({title:t,description:el('desc').value})});el('title').value='';el('desc').value='';refresh();};

el('random').onclick=()=>{useRandom=!useRandom;if(useRandom){impersonationSeed=newSeed();el('random').innerText='Stop impersonating';}else{impersonationSeed=null;el('random').innerText='Impersonate random user';}refresh();};
el('refresh').onclick=()=>{if(useRandom)impersonationSeed=newSeed();refresh();};
refresh();
document.getElementById('badge').onclick=()=>{useRandom=false;impersonationSeed=null;refresh();};
</script>
</body></html>
""", mimetype="text/html")

# API endpoints
@app.route("/api/experiment")
def api_experiment():
    user_id, statsig_user = make_user_obj()
    color, paragraph, raw = get_experiment_for_user(user_id, statsig_user, force_demo=bool(request.args.get('random')))
    return jsonify({"color": color, "paragraph": paragraph, "raw": raw, "user_id": user_id})

@app.route("/api/tasks", methods=["GET","POST"])
def api_tasks():
    if request.method=='GET': return jsonify(tasks)
    data=request.get_json(silent=True) or {}
    title=data.get('title')
    if not title: return jsonify({'error':'title required'}),400
    task={"id":next(id_counter),"title":title,"description":data.get('description',''),"done":False}
    tasks.append(task)
    return jsonify(task),201

@app.route("/api/tasks/<int:task_id>",methods=["PUT","DELETE"])
def api_task(task_id):
    t=next((x for x in tasks if x['id']==task_id),None)
    if not t: return jsonify({'error':'not found'}),404
    if request.method=='DELETE': tasks[:] = [x for x in tasks if x['id'] != task_id]; return jsonify({'result':True})
    data=request.get_json(silent=True) or {}
    if 'title' in data: t['title']=data['title']
    if 'description' in data: t['description']=data['description']
    if 'done' in data: t['done']=bool(data['done'])
    return jsonify(t)

if __name__=="__main__":
    app.run(debug=True)
