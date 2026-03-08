import os
import unittest
from datetime import datetime, timedelta

import pytz

import scanner_worker as sw


class ScannerWorkerLogicTests(unittest.TestCase):
    def test_normalize_alert_profile(self):
        self.assertEqual(sw._normalize_alert_profile("conservative"), "conservador")
        self.assertEqual(sw._normalize_alert_profile("aggressive"), "agresivo")
        self.assertEqual(sw._normalize_alert_profile("otro"), "balanceado")

    def test_apply_profile_conservador(self):
        cfg = {
            "alert_profile": "conservador",
            "min_confidence_score": 80,
            "min_rr": 1.5,
            "min_mtf_confirmations": 1,
            "require_price_action_confirmation": False,
            "persistence_bars": 1,
        }
        out = sw._apply_profile_to_precision_cfg(cfg)
        self.assertEqual(out["alert_profile"], "conservador")
        self.assertGreaterEqual(out["min_confidence_score"], 85)
        self.assertGreaterEqual(out["min_rr"], 1.8)
        self.assertGreaterEqual(out["min_mtf_confirmations"], 2)
        self.assertTrue(out["require_price_action_confirmation"])
        self.assertGreaterEqual(out["persistence_bars"], 2)

    def test_apply_profile_agresivo(self):
        cfg = {
            "alert_profile": "agresivo",
            "min_confidence_score": 90,
            "min_rr": 2.0,
            "min_mtf_confirmations": 2,
            "require_price_action_confirmation": True,
            "persistence_bars": 2,
        }
        out = sw._apply_profile_to_precision_cfg(cfg)
        self.assertEqual(out["alert_profile"], "agresivo")
        self.assertLessEqual(out["min_confidence_score"], 78)
        self.assertLessEqual(out["min_rr"], 1.5)
        self.assertEqual(out["min_mtf_confirmations"], 1)
        self.assertFalse(out["require_price_action_confirmation"])
        self.assertEqual(out["persistence_bars"], 1)

    def test_global_quality_calibration_tighten_hard(self):
        cfg = {
            "enabled": True,
            "min_confidence_score": 80,
            "min_rr": 1.6,
            "min_mtf_confirmations": 1,
            "quality_calibration_enabled": True,
            "quality_calibration_min_resolved": 5,
        }
        state = {
            "symbols": {
                "A": {"quality_stats": {"wins": 4, "losses": 8, "timeouts": 8}},
            }
        }
        out = sw._apply_quality_calibration(cfg, state)
        self.assertEqual(out["quality_calibration"]["mode"], "tighten_hard")
        self.assertEqual(out["min_confidence_score"], 84)
        self.assertEqual(out["min_rr"], 1.8)
        self.assertEqual(out["min_mtf_confirmations"], 2)

    def test_record_quality_calibration_applies(self):
        cfg = {
            "enabled": True,
            "min_confidence_score": 80,
            "min_rr": 1.6,
            "min_mtf_confirmations": 1,
            "quality_calibration_enabled": True,
            "quality_calibration_scope": "global_and_record",
            "quality_calibration_record_enabled": True,
            "quality_calibration_record_min_resolved": 8,
            "quality_calibration": {"mode": "neutral"},
        }
        record = {"quality_stats": {"wins": 2, "losses": 4, "timeouts": 2}}
        out = sw._apply_record_quality_calibration(cfg, record, "Cripto|BTC|BTC-USD|15m")
        self.assertEqual(out["quality_calibration"]["scope"], "global_and_record")
        self.assertIn(out["quality_calibration"]["mode"], {"tighten_soft", "tighten_hard"})
        self.assertGreaterEqual(out["min_confidence_score"], 81)
        self.assertGreaterEqual(out["min_rr"], 1.65)

    def test_signal_strength_label(self):
        strong = sw._signal_strength_label(
            {"confidence_score": 92, "min_confidence_required": 84},
            {"rr_estimado": 2.2},
        )
        weak = sw._signal_strength_label(
            {"confidence_score": 85, "min_confidence_required": 84},
            {"rr_estimado": 1.6},
        )
        self.assertEqual(strong, "FUERTE")
        self.assertEqual(weak, "DEBIL")

    def test_should_alert_respects_cooldown(self):
        record = {
            "dorado_streak": 2,
            "dorado_active": False,
            "last_alert_utc": "",
        }
        self.assertTrue(
            sw._should_alert(
                record=record,
                signal_ready=True,
                cooldown_minutes=60,
                persistence_bars=2,
                max_alerts_per_symbol_day=0,
            )
        )
        record["last_alert_utc"] = (
            datetime.now(pytz.UTC) - timedelta(minutes=10)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertFalse(
            sw._should_alert(
                record=record,
                signal_ready=True,
                cooldown_minutes=60,
                persistence_bars=2,
                max_alerts_per_symbol_day=0,
            )
        )

    def test_redact_text_hides_sensitive_values(self):
        key = "TELEGRAM_BOT_TOKEN"
        prev = os.environ.get(key)
        os.environ[key] = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"
        try:
            msg = "Error para 6751045178: token 123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ1234 correo x@y.com"
            redacted = sw._redact_text(msg)
            self.assertNotIn("6751045178", redacted)
            self.assertNotIn("123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ1234", redacted)
            self.assertNotIn("x@y.com", redacted)
            self.assertIn("redacted", redacted.lower())
        finally:
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


if __name__ == "__main__":
    unittest.main()

