# Pickleball 26 online server
Quick match by OVR, friends (presence + invites) and the match relay for Pickleball 26.
Run locally: `pip install -r requirements.txt && python server.py` (port 8765, or $PORT).
Deploy: Render "Blueprint" from this repo (render.yaml), free plan.
Parties + online doubles: see the protocol notes at the top of server.py. Test locally: `python test_party.py`
(starts its own server on port 8792 and stops it).
