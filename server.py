"""Pickleball 26 online service: quick match by OVR, friends (presence + invites), parties + doubles, and a match relay.

One WebSocket per player. Text frames are JSON control messages; binary frames are game traffic. No database: friend
lists live on each player's PC and are sent on connect, so a restart (or the free tier napping) loses nothing but the
current queues, parties and matches.

Binary relay:
  1v1 room      -> forwarded untouched to the opponent (unchanged since v0.4).
  4-player room -> forwarded to the other 3 members PREFIXED with one byte = the sender's slot (0..3).
  party (no room) -> forwarded untouched to the other party member(s) that aren't in a room (profiles, party voice).

Client -> server (JSON):
  hello {id, code, name, ovr, friends:[codes], version}
  queue {ovr} / unqueue                       1v1 quick match
  friend_add {code} / friend_remove {code}
  invite {code}                               -> target gets invite {room, code, name, ovr}
  invite_reply {room, accept}
  leave                                       leave the current match (1v1 or 4-player)
  ping
  party_invite {code}                         -> target gets party_invite {party, code, name, ovr}
  party_join {party} / party_decline {party} / party_leave / party_kick {code} (leader)
  party_ready {ready, ovr?}                   all members of a full party ready -> queued for doubles
  queue2 {ovr} / unqueue2                     doubles queue (solo, or a party: marks the sender ready)
  back_to_party                               host: end the 4-player room, everyone returns to their party
Server -> client (JSON):
  welcome {version, online:[codes]}, presence {code, online}
  matched {room, role: host|guest, opponent: {name, code, ovr}, private: bool}
  invite {...}, invite_declined {code}, friend_request {code, name}
  opponent_left, error {msg}, pong
  party {party, leader, queued, members:[{code, name, ovr, ready}]}   (party=null, members=[] when you're out of it;
                                                                      kicked=true if the leader kicked you)
  party_invite {party, code, name, ovr}, party_declined {code, name}
  queued2 {queued}
  matched4 {room, slot, role: host|guest, players:[{slot, code, name, ovr, team}]}
      slot 0 = host / team A main, 1 = team A partner, 2 = team B main, 3 = team B partner
  player_left {slot}, room_closed, back_to_party
"""
import asyncio, json, os, secrets, time
import http
from itertools import combinations
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

VERSION = 1
PARTY_MAX = 2
players = {}          # code -> Player
queue = {}            # code -> time joined (1v1)
rooms = {}            # room -> [Player, Player]
pending_invites = {}  # room -> (inviter, target)
parties = {}          # party id -> Party
queue2 = {}           # unit key -> {"t", "members": [Player], "party": Party|None}
rooms4 = {}           # room -> [Player|None] * 4


class Player:
    def __init__(self, ws):
        self.ws = ws
        self.code = ""
        self.name = "PLAYER"
        self.ovr = 50
        self.friends = set()
        self.room = None
        self.partner = None
        self.party = None
        self.ready = False
        self.room4 = None
        self.slot = -1

    async def send(self, **msg):
        try:
            await self.ws.send(json.dumps(msg))
        except Exception:
            pass


class Party:
    def __init__(self, leader):
        self.id = secrets.token_hex(4)
        self.leader = leader.code
        self.members = [leader]
        self.invites = set()


def watchers_of(code):
    return [p for p in players.values() if code in p.friends]


async def presence(code, online):
    for p in watchers_of(code):
        await p.send(type="presence", code=code, online=online)


def free(p):
    return p.room is None and p.room4 is None


# ---------------------------------------------------------------- 1v1
async def start_match(a, b, private):
    room = secrets.token_hex(4)
    for p in (a, b):
        queue.pop(p.code, None)
        await unqueue2(p)
        p.room = room
    a.partner, b.partner = b, a
    rooms[room] = [a, b]
    # the first player runs the match simulation, the other draws it
    await a.send(type="matched", room=room, role="host", private=private, opponent=dict(name=b.name, code=b.code, ovr=b.ovr))
    await b.send(type="matched", room=room, role="guest", private=private, opponent=dict(name=a.name, code=a.code, ovr=a.ovr))


async def leave_room(p, notify=True):
    if p.room is None:
        return
    room = rooms.pop(p.room, None)
    other = p.partner
    p.room, p.partner = None, None
    if other is not None:
        other.room, other.partner = None, None
        if notify:
            await other.send(type="opponent_left")


async def matcher_1v1(now):
    """Pair waiting players by closest OVR; the allowed gap widens the longer someone waits.
    With only a couple of people searching, everyone ends up paired with whoever is closest."""
    waiting = sorted((c for c in queue if c in players), key=lambda c: queue[c])
    taken = set()
    for c in waiting:
        if c in taken:
            continue
        me = players[c]
        win_me = 4 + 3 * (now - queue[c])
        best, best_d = None, 1e9
        for o in waiting:
            if o == c or o in taken:
                continue
            d = abs(players[o].ovr - me.ovr)
            if d < best_d:
                best, best_d = o, d
        if best is None:
            continue
        win = max(win_me, 4 + 3 * (now - queue[best]))
        if best_d <= win:
            taken.update((c, best))
            await start_match(me, players[best], False)


# ---------------------------------------------------------------- parties
def unit_key(p):
    return ("p:" + p.party.id) if p.party is not None else ("s:" + p.code)


def party_msg(party):
    return dict(type="party", party=party.id, leader=party.leader, queued=("p:" + party.id) in queue2,
                members=[dict(code=m.code, name=m.name, ovr=m.ovr, ready=m.ready) for m in party.members])


async def party_broadcast(party):
    if party is None or not party.members:
        return
    msg = party_msg(party)
    for m in party.members:
        await m.send(**msg)


async def no_party(p, **extra):
    await p.send(type="party", party=None, leader="", queued=False, members=[], **extra)


async def set_queued(members, on):
    for m in members:
        await m.send(type="queued2", queued=on)


async def unqueue2(p, reset_ready=False):
    """Take p's unit (their party, or just them) out of the doubles queue."""
    keys = [unit_key(p), "s:" + p.code]
    for k in keys:
        u = queue2.pop(k, None)
        if u is not None:
            if reset_ready:
                for m in u["members"]:
                    m.ready = False
            await set_queued(u["members"], False)
            if u["party"] is not None:
                await party_broadcast(u["party"])


async def leave_party(p, notify_self=True, **extra):
    party = p.party
    if party is None:
        return
    await unqueue2(p)
    party.members.remove(p)
    p.party, p.ready = None, False
    if notify_self:
        await no_party(p, **extra)
    if not party.members:
        parties.pop(party.id, None)
        return
    if party.leader == p.code:
        party.leader = party.members[0].code
    for m in party.members:
        m.ready = False
    await party_broadcast(party)


def ensure_party(p):
    if p.party is None:
        party = Party(p)
        parties[party.id] = party
        p.party = party
        p.ready = False
    return p.party


async def try_queue_party(party):
    """Queue a full party for doubles once everyone is ready and free."""
    key = "p:" + party.id
    if key in queue2 or len(party.members) < PARTY_MAX:
        return
    if not all(m.ready and free(m) for m in party.members):
        return
    for m in party.members:
        queue.pop(m.code, None)
    lead = [m for m in party.members if m.code == party.leader] + [m for m in party.members if m.code != party.leader]
    queue2[key] = dict(t=time.time(), members=lead, party=party)
    await set_queued(lead, True)


# ---------------------------------------------------------------- doubles (4-player rooms)
def _avg(ps):
    return sum(p.ovr for p in ps) / len(ps)


def split_teams(units):
    """units (oldest first) -> (team A, team B, OVR gap). A party always stays together; team A gets the first party."""
    duos = [u["members"] for u in units if len(u["members"]) == 2]
    solos = [u["members"][0] for u in units if len(u["members"]) == 1]
    if len(duos) == 2:
        a, b = duos[0], duos[1]
    elif len(duos) == 1:
        a, b = duos[0], solos
    else:
        best = None
        for j in (1, 2, 3):
            a = [solos[0], solos[j]]
            b = [s for k, s in enumerate(solos) if k not in (0, j)]
            d = abs(_avg(a) - _avg(b))
            if best is None or d < best[2]:
                best = (a, b, d)
        return best
    return a, b, abs(_avg(a) - _avg(b))


def unit_valid(key, u):
    for m in u["members"]:
        if players.get(m.code) is not m or not free(m):
            return False
    party = u["party"]
    if party is not None and (parties.get(party.id) is not party or len(party.members) != len(u["members"])):
        return False
    return True


async def start_match4(units, a, b):
    room = secrets.token_hex(4)
    slots = [a[0], a[1], b[0], b[1]]
    for u in units:
        for k, v in list(queue2.items()):
            if v is u:
                queue2.pop(k, None)
    rooms4[room] = list(slots)
    for i, p in enumerate(slots):
        queue.pop(p.code, None)
        p.room4, p.slot, p.ready = room, i, False
    info = [dict(slot=i, code=p.code, name=p.name, ovr=p.ovr, team=i // 2) for i, p in enumerate(slots)]
    for i, p in enumerate(slots):
        await p.send(type="matched4", room=room, slot=i, role="host" if i == 0 else "guest", players=info)


async def matcher_2v2(now):
    for k, u in list(queue2.items()):
        if not unit_valid(k, u):
            queue2.pop(k, None)
            await set_queued([m for m in u["members"] if players.get(m.code) is m], False)
    units = sorted(queue2.values(), key=lambda u: u["t"])
    taken = set()
    for u in units:
        live = [x for x in units if id(x) not in taken and any(v is x for v in queue2.values())]
        if not any(x is u for x in live):
            continue
        need = 4 - len(u["members"])
        others = [o for o in live if o is not u][:30]
        best = None
        for k in (1, 2, 3):
            for combo in combinations(others, k):
                if sum(len(o["members"]) for o in combo) != need:
                    continue
                group = sorted([u] + list(combo), key=lambda x: x["t"])
                a, b, d = split_teams(group)
                if best is None or d < best[0]:
                    best = (d, group, a, b)
        if best is None:
            continue
        d, group, a, b = best
        wait = max(now - g["t"] for g in group)
        win = 4 + 3 * wait
        # only this one group is possible (e.g. just two parties searching): match it after a short wait
        if d <= win or (sum(len(x["members"]) for x in live) == 4 and wait >= 5):
            taken.update(id(g) for g in group)
            await start_match4(group, a, b)


async def end_room4(room, exclude=None, msg="room_closed"):
    members = rooms4.pop(room, None) or []
    for m in members:
        if m is not None:
            m.room4, m.slot = None, -1
    for m in members:
        if m is not None and m is not exclude:
            await m.send(type=msg)
    for party in {id(m.party): m.party for m in members if m is not None and m.party is not None}.values():
        await party_broadcast(party)


async def leave_room4(p):
    room = p.room4
    if room is None:
        return
    members = rooms4.get(room)
    slot = p.slot
    if members is None:
        p.room4, p.slot = None, -1
        return
    if slot == 0:
        await end_room4(room, exclude=p)
        return
    members[slot] = None
    p.room4, p.slot = None, -1
    for m in members:
        if m is not None:
            await m.send(type="player_left", slot=slot)
    if all(m is None for m in members):
        rooms4.pop(room, None)
    await party_broadcast(p.party)


async def matcher():
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        await matcher_1v1(now)
        try:
            await matcher_2v2(now)
        except Exception as e:  # never let a doubles bug stop 1v1 matching
            print("matcher_2v2 error", repr(e), flush=True)


async def drop_player(p):
    """p's connection is gone (or replaced): leave every queue, match and party."""
    queue.pop(p.code, None)
    await unqueue2(p)
    await leave_room(p)
    await leave_room4(p)
    await leave_party(p, notify_self=False)


# ---------------------------------------------------------------- connection
async def handle(ws):
    me = Player(ws)
    try:
        async for msg in ws:
            if isinstance(msg, bytes):
                if me.partner is not None:
                    try:
                        await me.partner.ws.send(msg)
                    except Exception:
                        pass
                elif me.room4 is not None:
                    members = rooms4.get(me.room4) or []
                    out = bytes([me.slot & 0xFF]) + msg
                    for m in members:
                        if m is not None and m is not me:
                            try:
                                await m.ws.send(out)
                            except Exception:
                                pass
                elif me.party is not None:
                    for m in me.party.members:
                        if m is not me and free(m):
                            try:
                                await m.ws.send(msg)
                            except Exception:
                                pass
                continue
            try:
                m = json.loads(msg)
            except Exception:
                continue
            if not isinstance(m, dict):
                continue
            t = m.get("type")
            if os.environ.get("PB_DEBUG"):
                print("msg", me.code or "?", t, flush=True)
            if t == "hello":
                code = str(m.get("code", ""))[:12].upper()
                if not code:
                    await me.send(type="error", msg="no code")
                    continue
                old = players.get(code)
                if old is not None and old is not me:
                    await drop_player(old)
                    try:
                        await old.ws.close()
                    except Exception:
                        pass
                me.code = code
                me.name = str(m.get("name", "PLAYER"))[:20]
                me.ovr = int(m.get("ovr", 50))
                me.friends = set(str(f).upper() for f in m.get("friends", []))
                players[code] = me
                await me.send(type="welcome", version=VERSION, online=[f for f in me.friends if f in players])
                await presence(code, True)
            elif not me.code:
                continue
            elif t == "ping":
                await me.send(type="pong")
            elif t == "queue":
                me.ovr = int(m.get("ovr", me.ovr))
                await leave_room(me)
                await leave_room4(me)
                await unqueue2(me, reset_ready=True)
                queue[me.code] = time.time()
            elif t == "unqueue":
                queue.pop(me.code, None)
            elif t == "friend_add":
                code = str(m.get("code", "")).upper()
                me.friends.add(code)
                other = players.get(code)
                if other is not None:
                    await other.send(type="friend_request", code=me.code, name=me.name)
                    await me.send(type="presence", code=code, online=True)
                else:
                    await me.send(type="presence", code=code, online=False)
            elif t == "friend_remove":
                me.friends.discard(str(m.get("code", "")).upper())
            elif t == "invite":
                other = players.get(str(m.get("code", "")).upper())
                if other is None:
                    await me.send(type="error", msg="Your friend isn't online.")
                    continue
                room = secrets.token_hex(4)
                pending_invites[room] = (me.code, other.code)
                await other.send(type="invite", room=room, code=me.code, name=me.name, ovr=me.ovr)
            elif t == "invite_reply":
                inv = pending_invites.pop(str(m.get("room", "")), None)
                if inv is None:
                    continue
                host, target = players.get(inv[0]), players.get(inv[1])
                if host is None or target is not me:
                    continue
                if m.get("accept"):
                    await leave_room(host)
                    await leave_room(me)
                    await leave_room4(host)
                    await leave_room4(me)
                    await start_match(host, me, True)
                else:
                    await host.send(type="invite_declined", code=me.code)
            elif t == "leave":
                await leave_room(me)
                await leave_room4(me)
            # ---------------- parties + doubles
            elif t == "party_invite":
                other = players.get(str(m.get("code", "")).upper())
                if other is None or other is me:
                    await me.send(type="error", msg="That player isn't online.")
                    continue
                party = ensure_party(me)
                if len(party.members) >= PARTY_MAX:
                    await me.send(type="error", msg="Your party is full.")
                    continue
                if other in party.members:
                    continue
                party.invites.add(other.code)
                await party_broadcast(party)
                await other.send(type="party_invite", party=party.id, code=me.code, name=me.name, ovr=me.ovr)
            elif t == "party_join":
                party = parties.get(str(m.get("party", "")))
                if party is None or me.code not in party.invites:
                    await me.send(type="error", msg="That party invite has expired.")
                    continue
                if me.party is party:
                    continue
                if len(party.members) >= PARTY_MAX:
                    await me.send(type="error", msg="That party is full.")
                    continue
                party.invites.discard(me.code)
                await leave_party(me, notify_self=False)
                await unqueue2(me, reset_ready=True)
                queue2.pop("p:" + party.id, None)
                for x in party.members:
                    x.ready = False
                party.members.append(me)
                me.party, me.ready = party, False
                await set_queued(party.members, False)
                await party_broadcast(party)
            elif t == "party_decline":
                party = parties.get(str(m.get("party", "")))
                if party is None or me.code not in party.invites:
                    continue
                party.invites.discard(me.code)
                leader = players.get(party.leader)
                if leader is not None:
                    await leader.send(type="party_declined", code=me.code, name=me.name)
            elif t == "party_leave":
                if me.party is None:
                    await no_party(me)
                else:
                    await leave_party(me)
            elif t == "party_kick":
                party = me.party
                code = str(m.get("code", "")).upper()
                if party is None or party.leader != me.code or code == me.code:
                    continue
                target = next((x for x in party.members if x.code == code), None)
                if target is not None:
                    await leave_party(target, kicked=True)
            elif t == "party_ready":
                if "ovr" in m:
                    me.ovr = int(m.get("ovr", me.ovr))
                party = ensure_party(me)
                me.ready = bool(m.get("ready", False))
                if not me.ready:
                    await unqueue2(me)
                else:
                    await try_queue_party(party)
                await party_broadcast(party)
            elif t == "queue2":
                me.ovr = int(m.get("ovr", me.ovr))
                await leave_room(me)
                await leave_room4(me)
                queue.pop(me.code, None)
                party = me.party
                if party is not None and len(party.members) >= 2:
                    me.ready = True
                    await try_queue_party(party)
                    await party_broadcast(party)
                else:
                    key = unit_key(me)
                    if key not in queue2:
                        me.ready = True
                        queue2[key] = dict(t=time.time(), members=[me], party=party)
                    await set_queued([me], True)
                    await party_broadcast(party)
            elif t == "unqueue2":
                await unqueue2(me, reset_ready=True)
            elif t == "back_to_party":
                if me.room4 is None:
                    await party_broadcast(me.party)
                elif me.slot == 0:
                    await end_room4(me.room4, exclude=me, msg="back_to_party")
                else:
                    await leave_room4(me)
    except ConnectionClosed:
        pass
    finally:
        if me.code and players.get(me.code) is me:
            players.pop(me.code, None)
            await drop_player(me)
            await presence(me.code, False)


def health(connection, request):
    # Render (and browsers) poke the URL over plain HTTP: answer so health checks pass
    if "Sec-WebSocket-Key" not in request.headers:
        msg = "Pickleball 26 server OK - %d online, %d searching, %d matches, %d parties, %d doubles searching, %d doubles matches" % (
            len(players), len(queue), len(rooms), len(parties), sum(len(u["members"]) for u in queue2.values()), len(rooms4))
        return connection.respond(http.HTTPStatus.OK, msg + chr(10))
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handle, "0.0.0.0", port, process_request=health, max_size=4 * 1024 * 1024,
                                ping_interval=20, ping_timeout=40):
        print("Pickleball 26 server on port", port, flush=True)
        await matcher()


if __name__ == "__main__":
    asyncio.run(main())
