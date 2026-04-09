import unittest

import analysis


class AnalysisPullbackTests(unittest.TestCase):
    def test_score_azul_uses_structure_window_by_interval(self):
        df = analysis.pd.DataFrame(
            {
                "High": [100 + i * 0.3 for i in range(24)],
                "Low": [99 + i * 0.2 for i in range(24)],
                "Close": [99.5 + i * 0.25 for i in range(24)],
                "EMA_20": [99 + i * 0.2 for i in range(24)],
                "EMA_50": [98 + i * 0.15 for i in range(24)],
                "EMA_200": [95.0] * 24,
                "RSI": [56.0] * 24,
            }
        )

        out_15m = analysis.calcular_score_azul(df, interval="15m")
        out_1d = analysis.calcular_score_azul(df, interval="1d")

        self.assertEqual(out_15m["structure_window"], 12)
        self.assertEqual(out_1d["structure_window"], 20)
        self.assertEqual(out_15m["structure_window_effective"], 12)
        self.assertEqual(out_1d["structure_window_effective"], 12)

    def test_classify_pullback_tendencia_alcista(self):
        setup = analysis._classify_dorado_setup(
            direccion="ALCISTA",
            close=105.1,
            ema20=105.0,
            ema50=104.2,
            ema200=101.5,
            atr=1.0,
            soporte=104.8,
            resistencia=108.5,
        )
        self.assertEqual(setup["setup_tipo"], "pullback_tendencia")
        self.assertEqual(setup["setup_label"], "Pullback en tendencia")
        self.assertTrue(setup["tendencia_alineada"])
        self.assertIn(setup["zona_pullback"], {"EMA20", "EMA50", "soporte"})

    def test_classify_pullback_requires_aligned_trend(self):
        setup = analysis._classify_dorado_setup(
            direccion="ALCISTA",
            close=105.1,
            ema20=104.0,
            ema50=104.5,
            ema200=101.5,
            atr=1.0,
            soporte=104.8,
            resistencia=108.5,
        )
        self.assertEqual(setup["setup_tipo"], "")
        self.assertFalse(setup["tendencia_alineada"])


if __name__ == "__main__":
    unittest.main()
