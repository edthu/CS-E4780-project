class StreamsAppSuite extends munit.FunSuite:
  test("calculateEma uses the EMA smoothing formula"):
    // EMA_{s,w0} = 0: the first window is weighted against zero, not seeded.
    val first = StreamsApp.calculateEma(0.0, 100.0, 38)
    assert(Math.abs(first - 100.0 * 2.0 / 39.0) < 1e-12)

    val next = StreamsApp.calculateEma(100.0, 110.0, 38)
    val alpha = 2.0 / (38.0 + 1.0)
    val expected = alpha * 110.0 + (1.0 - alpha) * 100.0
    assert(Math.abs(next - expected) < 1e-12)

  test("detectSignal emits BUY and SELL purely on EMA crossovers"):
    assertEquals(StreamsApp.detectSignal(99.0, 100.0, 101.0, 99.0), Some("BUY"))
    assertEquals(StreamsApp.detectSignal(101.0, 99.0, 100.0, 101.0), Some("SELL"))
    assertEquals(StreamsApp.detectSignal(101.0, 99.0, 101.5, 98.0), None)
