import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
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

    def test_render_512mb_profile_compacts_watchlist(self):
        cfg = {
            "resource_profile": "render_512mb",
            "scan_crypto": True,
            "scan_gold": True,
            "scan_structural_1d_4h": True,
            "scan_intervals": ["15m", "30m", "1h", "4h"],
            "poll_interval_sec": 60,
            "crypto_symbols": ["BTC", "ETH", "SOL", "BNB", "XRP"],
            "gold_symbols": ["XAU/USD"],
            "runtime_limits": {},
        }
        out = sw._apply_resource_profile(cfg)
        self.assertEqual(out["resource_profile"], "render_512mb")
        self.assertFalse(out["scan_structural_1d_4h"])
        self.assertEqual(out["scan_intervals"], ["15m", "1h"])
        self.assertGreaterEqual(out["poll_interval_sec"], 90)
        self.assertEqual(out["crypto_symbols"], ["BTC", "ETH", "SOL", "BNB"])

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
            {
                "confidence_score": 92,
                "min_confidence_required": 84,
                "candle_pattern": "rechazo_alcista",
                "riesgo": "Moderado",
                "indice_alerta_utc": "2026-03-10T15:00:00Z",
                "mtf_summary": "confirmaciones=1, opuestos=0, neutrales=0",
            },
            {"rr_estimado": 2.3, "micro_score": 6, "umbral": 4},
        )
        borderline = sw._signal_strength_label(
            {
                "confidence_score": 89,
                "min_confidence_required": 80,
                "candle_pattern": "rechazo_alcista",
                "riesgo": "Moderado",
                "indice_alerta_utc": "2026-03-10T15:00:00Z",
                "mtf_summary": "confirmaciones=1, opuestos=0, neutrales=1",
            },
            {"rr_estimado": 2.2, "micro_score": 5, "umbral": 4},
        )
        weak = sw._signal_strength_label(
            {
                "confidence_score": 85,
                "min_confidence_required": 84,
                "candle_pattern": "sin_patron",
                "riesgo": "Moderado",
                "indice_alerta_utc": "2026-03-10T15:00:00Z",
                "mtf_summary": "confirmaciones=0, opuestos=0, neutrales=1",
            },
            {"rr_estimado": 1.6, "micro_score": 4, "umbral": 4},
        )
        self.assertEqual(strong, "FUERTE")
        self.assertEqual(borderline, "DEBIL")
        self.assertEqual(weak, "DEBIL")

    def test_build_alert_payload_includes_pullback_metadata(self):
        cfg = {"notification": {"subject_prefix": "[Estrella Trader]"}}
        item = sw.MarketItem(
            market="Cripto",
            label="BTC",
            ticker="BTC-USD",
            td_symbol="BTC/USD",
            kind="crypto",
        )
        estado = {
            "dorado_v13": {
                "micro_score": 6,
                "umbral": 4,
                "rr_estimado": 2.3,
                "setup_tipo": "pullback_tendencia",
                "setup_label": "Pullback en tendencia",
                "zona_pullback": "EMA20",
            },
            "riesgo": "Moderado",
            "decision": "OPERAR CON DISCIPLINA",
            "direccion_v13": "ALCISTA",
            "temporalidad_alerta": "15m",
            "modo_alerta": "Tendencial",
            "precio_alerta": 102345.12,
            "indice_alerta_utc": "2026-03-10T15:00:00Z",
            "confidence_score": 90,
            "min_confidence_required": 72,
            "candle_pattern": "rechazo_alcista",
            "mtf_summary": "confirmaciones=1, opuestos=0, neutrales=0",
            "alert_profile": "balanceado",
            "operational_plan": {"rr_ratio": 2.0},
        }
        subject, body = sw._build_alert_payload(cfg, item, estado, "source")
        self.assertIn("BTC-USD | 15m", subject)
        self.assertIn("Estado de la sesion: Optima", body)
        self.assertIn("Recomendacion: Operar normal", body)
        self.assertIn("Hora Col: 2026-03-10 10:00", body)
        self.assertIn("Direccion: ALCISTA", body)
        self.assertIn("Escenario: Continuacion tendencial confirmada", body)
        self.assertIn("Entrada guia: 102345.12", body)
        self.assertIn("Puntaje tecnico: 90/100", body)
        self.assertIn("Checklist tecnico: 6/4", body)
        self.assertIn("Patron: rechazo_alcista", body)
        self.assertIn("Mentor:", body)
        self.assertIn("viernes y durante el fin de semana", body)

    def test_session_status_for_alert_marks_weekend_as_not_favorable(self):
        state, recommendation = sw._session_status_for_alert("2026-03-14T15:00:00Z")
        self.assertEqual(state, "No favorable")
        self.assertEqual(recommendation, "No operar")

    def test_session_status_for_alert_marks_friday_as_not_favorable(self):
        state, recommendation = sw._session_status_for_alert("2026-03-13T15:00:00Z")
        self.assertEqual(state, "No favorable")
        self.assertEqual(recommendation, "No operar")

    def test_fetch_data_prioritizes_binance_for_crypto(self):
        item = sw.MarketItem(
            market="Cripto",
            label="BTC",
            ticker="BTC-USD",
            td_symbol="BTC/USD",
            kind="crypto",
            binance_symbol="BTCUSDT",
        )
        binance_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [101.0, 102.0],
                "Low": [99.5, 100.5],
                "Close": [100.8, 101.7],
                "Volume": [10.0, 12.0],
            }
        )

        with (
            patch.object(sw, "fetch_klines", return_value=(binance_df, None)) as fetch_mock,
            patch.object(sw, "obtener_datos") as yf_mock,
        ):
            df, source, err = sw._fetch_data(item=item, period="5d", interval="15m", cfg={})

        self.assertEqual(source, "binance")
        self.assertEqual(err, "")
        self.assertFalse(df.empty)
        fetch_mock.assert_called_once_with("BTCUSDT", "15m", limit=500)
        yf_mock.assert_not_called()

    def test_fetch_data_falls_back_to_yfinance_when_binance_fails(self):
        item = sw.MarketItem(
            market="Cripto",
            label="BTC",
            ticker="BTC-USD",
            td_symbol="BTC/USD",
            kind="crypto",
            binance_symbol="BTCUSDT",
        )
        yf_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [101.0, 102.0],
                "Low": [99.5, 100.5],
                "Close": [100.8, 101.7],
                "Volume": [10.0, 12.0],
            }
        )

        with (
            patch.object(sw, "fetch_klines", return_value=(pd.DataFrame(), "sin datos")) as fetch_mock,
            patch.object(sw, "obtener_datos", return_value=yf_df) as yf_mock,
            patch.dict(os.environ, {"TWELVE_DATA_API_KEY": ""}, clear=False),
        ):
            df, source, err = sw._fetch_data(item=item, period="5d", interval="15m", cfg={})

        self.assertEqual(source, "yfinance")
        self.assertEqual(err, "")
        self.assertFalse(df.empty)
        fetch_mock.assert_called_once_with("BTCUSDT", "15m", limit=500)
        yf_mock.assert_called_once_with("BTC-USD", periodo="5d", intervalo="15m")

    def test_format_colombia_alert_time_uses_bogota_timezone(self):
        self.assertEqual(sw._format_colombia_alert_time("2026-03-28T19:35:00Z"), "2026-03-28 14:35")

    def test_resolve_operational_trade_plan_clamps_risk_to_minimum(self):
        estado = {"direccion_v13": "ALCISTA", "precio_alerta": 100.0}
        compute_ctx = {
            "df_ind": pd.DataFrame(
                {
                    "High": [100.2, 100.3, 100.25, 100.15, 100.1],
                    "Low": [99.95, 99.9, 99.85, 99.8, 99.82],
                }
            )
        }

        plan = sw._resolve_operational_trade_plan(estado=estado, compute_ctx=compute_ctx)

        self.assertTrue(plan["ok"])
        self.assertEqual(plan["risk_pct"], 0.5)
        self.assertEqual(plan["risk_pct_structural"], 0.2)
        self.assertTrue(plan["adjusted_to_min"])
        self.assertEqual(plan["sl_price"], 99.5)
        self.assertEqual(plan["tp_price"], 101.0)

    def test_resolve_operational_trade_plan_rejects_risk_above_maximum(self):
        estado = {"direccion_v13": "BAJISTA", "precio_alerta": 100.0}
        compute_ctx = {
            "df_ind": pd.DataFrame(
                {
                    "High": [100.5, 100.7, 100.9, 101.1, 101.3],
                    "Low": [99.8, 99.7, 99.6, 99.5, 99.4],
                }
            )
        }

        plan = sw._resolve_operational_trade_plan(estado=estado, compute_ctx=compute_ctx)

        self.assertFalse(plan["ok"])
        self.assertEqual(plan["risk_pct"], 1.3)
        self.assertIn("riesgo_operativo_fuera_marco", plan["reason"])

    def test_apply_precision_filters_allows_pullback_with_neutral_mtf(self):
        item = sw.MarketItem(
            market="Cripto",
            label="BTC",
            ticker="BTC-USD",
            td_symbol="BTC/USD",
            kind="crypto",
        )
        cfg = {"interval": "15m", "cooldown_minutes": 60}
        precision_cfg = {
            "enabled": True,
            "multi_timeframe_filter": True,
            "require_price_action_confirmation": False,
            "min_rr": 1.6,
            "min_confidence_score": 72,
            "min_mtf_confirmations": 1,
            "adaptive_threshold": False,
            "adaptive_cooldown": True,
        }
        estado = {
            "dorado_v13": {
                "rr_estimado": 2.4,
                "setup_tipo": "pullback_tendencia",
            },
            "setup_tipo": "pullback_tendencia",
            "direccion_v13": "ALCISTA",
            "precio_alerta": 100.0,
        }
        compute_ctx = {
            "interval": "15m",
            "vol_ratio": 1.0,
            "df_ind": pd.DataFrame(
                {
                    "High": [100.2, 100.3, 100.25, 100.15, 100.1],
                    "Low": [99.95, 99.9, 99.85, 99.8, 99.82],
                }
            ),
        }
        mtf_info = {
            "ok": False,
            "score": 0,
            "details": ["30m:NEUTRAL"],
            "summary": "confirmaciones=0, opuestos=0, neutrales=1",
            "confirmations": 0,
            "opposites": 0,
            "neutrals": 1,
        }
        candle_info = {"aligned": False, "bias": "ALCISTA", "pattern": "sin_patron", "score": 0}
        with (
            patch.object(sw, "_evaluate_mtf_alignment", return_value=mtf_info),
            patch.object(sw, "_detect_price_action", return_value=candle_info),
            patch.object(sw, "_compute_signal_confidence", return_value=68),
            patch.object(sw, "_compute_dynamic_min_confidence", return_value=72),
            patch.object(sw, "_compute_adaptive_cooldown_minutes", return_value=36),
        ):
            out = sw._apply_precision_filters(
                item=item,
                cfg=cfg,
                estado=estado,
                compute_ctx=compute_ctx,
                mtf_cache={},
                precision_cfg=precision_cfg,
            )
        self.assertTrue(out["signal_ready"])
        self.assertTrue(out["mtf"]["ok"])
        self.assertEqual(out["mtf"]["override"], "pullback_neutral")
        self.assertTrue(out["risk_ok"])
        self.assertEqual(out["reasons"], ["filtros_ok"])

    def test_apply_precision_filters_keeps_neutral_mtf_block_for_non_pullback(self):
        item = sw.MarketItem(
            market="Cripto",
            label="BTC",
            ticker="BTC-USD",
            td_symbol="BTC/USD",
            kind="crypto",
        )
        cfg = {"interval": "15m", "cooldown_minutes": 60}
        precision_cfg = {
            "enabled": True,
            "multi_timeframe_filter": True,
            "require_price_action_confirmation": False,
            "min_rr": 1.6,
            "min_confidence_score": 72,
            "min_mtf_confirmations": 1,
            "adaptive_threshold": False,
            "adaptive_cooldown": True,
        }
        estado = {
            "dorado_v13": {
                "rr_estimado": 2.4,
                "setup_tipo": "",
            },
            "setup_tipo": "",
            "direccion_v13": "ALCISTA",
            "precio_alerta": 100.0,
        }
        compute_ctx = {
            "interval": "15m",
            "vol_ratio": 1.0,
            "df_ind": pd.DataFrame(
                {
                    "High": [100.2, 100.3, 100.25, 100.15, 100.1],
                    "Low": [99.95, 99.9, 99.85, 99.8, 99.82],
                }
            ),
        }
        mtf_info = {
            "ok": False,
            "score": 0,
            "details": ["30m:NEUTRAL"],
            "summary": "confirmaciones=0, opuestos=0, neutrales=1",
            "confirmations": 0,
            "opposites": 0,
            "neutrals": 1,
        }
        candle_info = {"aligned": False, "bias": "ALCISTA", "pattern": "sin_patron", "score": 0}
        with (
            patch.object(sw, "_evaluate_mtf_alignment", return_value=mtf_info),
            patch.object(sw, "_detect_price_action", return_value=candle_info),
            patch.object(sw, "_compute_signal_confidence", return_value=68),
            patch.object(sw, "_compute_dynamic_min_confidence", return_value=72),
            patch.object(sw, "_compute_adaptive_cooldown_minutes", return_value=36),
        ):
            out = sw._apply_precision_filters(
                item=item,
                cfg=cfg,
                estado=estado,
                compute_ctx=compute_ctx,
                mtf_cache={},
                precision_cfg=precision_cfg,
            )
        self.assertFalse(out["signal_ready"])
        self.assertTrue(out["risk_ok"])
        self.assertIn("mtf_no_alineado", out["reasons"])
        self.assertIn("confianza_baja(<72)", out["reasons"])

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

    def test_persistence_allows_first_alert_on_second_bar(self):
        record = {
            "dorado_streak": 1,
            "dorado_active": False,
            "last_alert_utc": "",
        }
        self.assertFalse(
            sw._should_alert(
                record=record,
                signal_ready=True,
                cooldown_minutes=60,
                persistence_bars=2,
                max_alerts_per_symbol_day=0,
            )
        )
        record["dorado_active"] = sw._compute_dorado_active_state(
            signal_ready=True,
            current_streak=1,
            persistence_bars=2,
        )
        self.assertFalse(record["dorado_active"])
        record["dorado_streak"] = 2
        self.assertTrue(
            sw._should_alert(
                record=record,
                signal_ready=True,
                cooldown_minutes=60,
                persistence_bars=2,
                max_alerts_per_symbol_day=0,
            )
        )
        record["dorado_active"] = sw._compute_dorado_active_state(
            signal_ready=True,
            current_streak=2,
            persistence_bars=2,
        )
        self.assertTrue(record["dorado_active"])
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

    def test_compact_memory_sweep_calls_gc_and_trim(self):
        with (
            patch.object(sw.gc, "collect") as gc_collect,
            patch.object(sw, "_trim_process_memory") as trim_memory,
        ):
            sw._compact_memory_sweep()
        gc_collect.assert_called_once_with()
        trim_memory.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
