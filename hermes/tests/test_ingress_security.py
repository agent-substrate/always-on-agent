"""Socket-free tests of the real routes and authorization boundary."""
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import web
from aiohttp.test_utils import make_mocked_request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "gateway"))
import ingress


class IngressSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def test_trusted_adapter_boundary(self):
        app = ingress.build_app()
        with patch.dict(os.environ, {"API_SERVER_KEY": "actor-secret",
                                     "GATEWAY_INGRESS_KEY": "adapter-secret"}), \
             patch.object(ingress, "_proxy_turn", AsyncMock(
                 side_effect=lambda *args: web.json_response({"ok": True}))) as forward:
            for path in ("/message", "/v1/chat/completions"):
                for auth in ("", "Bearer actor-secret", "Bearer wrong", "adapter-secret"):
                    req = make_mocked_request("POST", path, headers={"Authorization": auth}, app=app)
                    req.json = AsyncMock(side_effect=ValueError("must not parse"))
                    route = await app.router.resolve(req)
                    with self.assertRaises(web.HTTPUnauthorized):
                        await route.handler(req)
                    req.json.assert_not_called()
                forward.assert_not_called()
                req = make_mocked_request("POST", path,
                    headers={"Authorization": "Bearer adapter-secret"}, app=app)
                req.json = AsyncMock(return_value={"session": "chosen", "messages": []})
                route = await app.router.resolve(req)
                self.assertEqual((await route.handler(req)).status, 200)
                forward.assert_awaited_once()
                forward.reset_mock()
            for key in ("", "actor-secret"):
                os.environ["GATEWAY_INGRESS_KEY"] = key
                for path in ("/message", "/v1/chat/completions"):
                    req = make_mocked_request("POST", path,
                        headers={"Authorization": "Bearer " + key}, app=app)
                    route = await app.router.resolve(req)
                    with self.assertRaises(web.HTTPServiceUnavailable):
                        await route.handler(req)
            forward.assert_not_called()

    def test_key_not_mounted_in_actor(self):
        self.assertNotIn("GATEWAY_INGRESS_KEY", (ROOT / "k8s/hermes-actortemplate.yaml").read_text())
