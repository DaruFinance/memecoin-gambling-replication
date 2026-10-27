"""Approximate return law of the Sweet Bonanza slot, shared by Studies 2 and 3.

The law has a point mass at X = 0, five tail values set from the 101rtp.net survival points and two mid-range values.
The upper mid-range value is solved so that the expected return equals the published RTP of 0.965.
The numbers are identical to s2_analyze.sb_law. Imported by other scripts; reads and writes nothing."""


def sb_law():
    p0 = 0.57063
    tail = {14.0: 0.007445, 40.0: 0.00153, 200.0: 0.000727, 700.0: 0.0000231, 1500.0: 0.00000232}
    rest = 1 - p0 - sum(tail.values())
    lo_mass, hi_mass = 0.255 / (0.255 + 0.165) * rest, 0.165 / (0.255 + 0.165) * rest
    tail_rtp = sum(v * p for v, p in tail.items())
    v_lo = 0.4
    v_hi = (0.965 - tail_rtp - v_lo * lo_mass) / hi_mass
    return {0.0: p0, v_lo: lo_mass, v_hi: hi_mass, **tail}
