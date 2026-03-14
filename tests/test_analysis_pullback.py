import unittest

import analysis


class AnalysisPullbackTests(unittest.TestCase):
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
