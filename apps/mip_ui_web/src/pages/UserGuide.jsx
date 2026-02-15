import './UserGuide.css'

export default function UserGuide() {
  return (
    <div className="user-guide">
      <h1>MIP User Guide</h1>
      <p className="guide-subtitle">
        Everything you need to know about the Market Intelligence Platform — explained
        for humans, with examples and illustrations.
      </p>

      {/* ── Table of contents ── */}
      <div className="guide-toc">
        <h3>Contents</h3>
        <div className="guide-toc-columns">
          <div className="guide-toc-col">
            <h4>Part 1 — How MIP Works</h4>
            <ol>
              <li><a href="#big-picture">The Big Picture</a></li>
              <li><a href="#pipeline">The Daily Pipeline</a></li>
              <li><a href="#signals">How Signals Are Generated</a></li>
              <li><a href="#outcomes">Outcome Evaluation (How Training Works)</a></li>
              <li><a href="#training-stages">Training Stages</a></li>
              <li><a href="#trust">Trust &amp; Eligibility</a></li>
              <li><a href="#hit-rate">What Is Hit Rate?</a></li>
              <li><a href="#avg-return">What Is Avg Return?</a></li>
              <li><a href="#trading">From Trust to Trading</a></li>
              <li><a href="#patterns">What Are Patterns?</a></li>
            </ol>
          </div>
          <div className="guide-toc-col">
            <h4>Part 2 — Page-by-Page Guide</h4>
            <ol start={11}>
              <li><a href="#page-home">Home</a> (11)</li>
              <li><a href="#page-cockpit">Cockpit (Daily Dashboard)</a> (12)</li>
              <li><a href="#page-portfolio">Portfolio Activity</a> (13)</li>
              <li><a href="#page-manage">Portfolio Management</a> (14)</li>
              <li><a href="#page-training">Training Status</a> (15)</li>
              <li><a href="#page-suggestions">Suggestions</a> (16)</li>
              <li><a href="#page-signals">Signals Explorer</a> (17)</li>
              <li><a href="#page-market-timeline">Market Timeline</a> (18)</li>
              <li><a href="#page-runs">Runs (Audit Viewer)</a> (19)</li>
              <li><a href="#page-debug">Debug</a> (20)</li>
              <li><a href="#page-parallel-worlds">Parallel Worlds</a> (21)</li>
            </ol>
          </div>
        </div>
      </div>

      {/* ═══════════════════════════════════════════════════════════════ */}
      {/*                PART 1 — HOW MIP WORKS                         */}
      {/* ═══════════════════════════════════════════════════════════════ */}
      <div className="guide-part-header">Part 1 — How MIP Works</div>

      {/* ─── 1. BIG PICTURE ─── */}
      <section className="guide-section" id="big-picture">
        <h2>1. The Big Picture</h2>
        <p>
          MIP is an automated market intelligence platform. It watches markets every day,
          detects interesting price movements, evaluates whether those patterns historically
          lead to profitable outcomes, and — only when confident — proposes trades.
        </p>
        <p>
          <strong>MIP does not predict tomorrow's price.</strong> Instead, it asks:
          "When this type of price action happened in the past, what usually followed?"
          It builds evidence over time and only acts when the evidence is strong enough.
        </p>

        <div className="guide-pipeline-flow">
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">📊</span>
              <div className="guide-pipeline-label">Market Data</div>
              <div className="guide-pipeline-desc">Fresh price bars arrive daily</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">🔍</span>
              <div className="guide-pipeline-label">Signal Detection</div>
              <div className="guide-pipeline-desc">Patterns scan for notable moves</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">🧪</span>
              <div className="guide-pipeline-label">Training</div>
              <div className="guide-pipeline-desc">Evaluate past signals, build evidence</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">✅</span>
              <div className="guide-pipeline-label">Trust Decision</div>
              <div className="guide-pipeline-desc">Enough evidence? Earn trust status</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">💹</span>
              <div className="guide-pipeline-label">Trade Proposals</div>
              <div className="guide-pipeline-desc">Only trusted patterns can trade</div>
            </div>
          </div>
        </div>

        <div className="guide-example">
          <div className="guide-example-title">Real-World Analogy</div>
          <p>
            Imagine you're hiring a weather forecaster. You wouldn't trust someone on day one.
            You'd watch them make predictions over weeks. After 40+ predictions, if they were
            right 75% of the time, you'd start to trust them. That's exactly how MIP works —
            it watches its own pattern detections, checks what actually happened, and only
            trusts patterns with a proven track record.
          </p>
        </div>
      </section>

      {/* ─── 2. THE DAILY PIPELINE ─── */}
      <section className="guide-section" id="pipeline">
        <h2>2. The Daily Pipeline</h2>
        <p>
          Every day, MIP runs an automated pipeline. Think of it as a checklist the system
          goes through:
        </p>
        <ol className="guide-numbered-steps">
          <li>
            <strong>Fetch new market bars</strong>
            <span className="guide-step-detail">Get the latest price data (open, high, low, close) for all tracked symbols — stocks, FX pairs, etc.</span>
          </li>
          <li>
            <strong>Generate signals</strong>
            <span className="guide-step-detail">Run each pattern definition to detect interesting price action in today's bars.</span>
          </li>
          <li>
            <strong>Evaluate old outcomes</strong>
            <span className="guide-step-detail">Look back at signals from previous days and check: did the price actually go up afterwards?</span>
          </li>
          <li>
            <strong>Update training metrics</strong>
            <span className="guide-step-detail">Recalculate hit rate, average return, and maturity score for every symbol/pattern combination.</span>
          </li>
          <li>
            <strong>Update trust labels</strong>
            <span className="guide-step-detail">Re-evaluate which patterns have earned TRUSTED status based on the latest metrics.</span>
          </li>
          <li>
            <strong>Generate trade proposals</strong>
            <span className="guide-step-detail">For trusted signals, propose buy or sell orders to the portfolio.</span>
          </li>
          <li>
            <strong>Execute trades</strong>
            <span className="guide-step-detail">Fill approved orders through the portfolio engine (if risk gate allows).</span>
          </li>
          <li>
            <strong>Generate AI digest</strong>
            <span className="guide-step-detail">Create a narrative summary of everything that happened — what changed, what matters, what to watch.</span>
          </li>
        </ol>
        <div className="guide-callout">
          <strong>Weekends &amp; Holidays</strong>
          If no new market data arrives (weekends, holidays), the pipeline still runs but
          skips signal generation. Training evaluation and digest generation still happen,
          so you'll always see a fresh AI narrative.
        </div>
      </section>

      {/* ─── 3. HOW SIGNALS WORK ─── */}
      <section className="guide-section" id="signals">
        <h2>3. How Signals Are Generated</h2>
        <p>
          A <strong>signal</strong> is not a prediction — it's a detection. Each "pattern"
          is a set of rules that looks for specific price behavior. When the rules match,
          a signal is logged.
        </p>

        <div className="guide-example">
          <div className="guide-example-title">Example: FX Pattern (Moving-Average Crossover)</div>
          <p>
            The <strong>FX_MOMENTUM_DAILY</strong> pattern for AUD/USD checks:
            "Did today's return exceed 0.1% AND is the price above both the 10-bar and
            20-bar moving averages?"
          </p>
          <p>
            If <strong>yes</strong> → a signal is logged with the observed return as its score.<br />
            If <strong>no</strong> → nothing happens. No signal, no record.
          </p>
          <p className="guide-example-numbers">
            Today AUD/USD closed at 0.6520, up from yesterday's 0.6502.<br />
            Daily return = (0.6520 − 0.6502) / 0.6502 = <strong>+0.277%</strong>.<br />
            The 0.277% exceeds the pattern's 0.1% minimum → signal fires with score 0.00277.
          </p>
        </div>

        <div className="guide-example">
          <div className="guide-example-title">Example: STOCK Pattern (Breakout + Momentum)</div>
          <p>
            The <strong>STOCK_MOMENTUM_FAST</strong> pattern for AAPL checks three conditions simultaneously:
          </p>
          <ol style={{marginLeft: '1.5rem', lineHeight: '1.7'}}>
            <li><strong>Minimum return:</strong> Did today's return exceed 0.2%?</li>
            <li><strong>Momentum confirmation (slow_window=1):</strong> Was yesterday also a green (positive) day?</li>
            <li><strong>Breakout (fast_window=5):</strong> Is today's close at a new 5-day high?</li>
            <li><strong>Z-score ≥ 1.0:</strong> Is today's move at least 1 standard deviation above the recent average (using 5-day volatility)?</li>
          </ol>
          <p>
            All four must be true simultaneously. This means the stock must be on consecutive green days,
            breaking out to new short-term highs, with an unusually large move.
          </p>
          <p className="guide-example-numbers">
            AAPL closed at $195.00 today, up from $193.50 yesterday (return = +0.78%).<br />
            Yesterday was also green (up from $192.00 → +0.78%). ✓ momentum check passed.<br />
            The highest close in the last 5 days was $194.20. Today's $195.00 exceeds it. ✓ breakout passed.<br />
            5-day return std dev = 0.5%. Z-score = 0.78% / 0.5% = 1.56. ✓ z-score ≥ 1.0.<br />
            → Signal fires with score 0.0078.
          </p>
        </div>

        <h3>Key parameters in each pattern</h3>
        <p>Every pattern has configuration parameters that control how selective it is. Here's what each one means:</p>
        <dl className="guide-kv guide-kv--wide">
          <dt>min_return</dt>
          <dd>
            <strong>What:</strong> Minimum observed return to fire a signal (e.g., 0.001 = 0.1%).<br />
            <strong>Why:</strong> Filters out tiny, insignificant moves. If a stock moved 0.01%, that's noise — not a signal.<br />
            <strong>Example:</strong> If min_return = 0.002 (0.2%), then AAPL going up 0.15% today would NOT fire a signal. AAPL going up 0.35% WOULD fire one.
          </dd>
          <dt>min_zscore</dt>
          <dd>
            <strong>What:</strong> Minimum z-score — how unusual the move is compared to the symbol's recent volatility.<br />
            <strong>Why:</strong> A 0.5% move might be huge for a stable stock but normal for a volatile one. Z-score adjusts for this.<br />
            <strong>How:</strong> Z-score = today's return ÷ standard deviation of returns over the <code>fast_window</code> period.
            For STOCK/ETF, the volatility is measured over the fast_window bars (e.g., 5 days for STOCK). For FX, it uses the fast_window bars as well.<br />
            <strong>Example:</strong> If AAPL's return std dev over the last 5 bars is 0.4% and today's return is 0.78%,
            then z-score = 0.78 / 0.4 = <strong>1.95</strong>. With min_zscore = 1.0, this signal fires. A move of only 0.3% (z-score = 0.75) would not.
          </dd>
          <dt>fast_window and slow_window</dt>
          <dd>
            <strong>Warning: These names are misleading.</strong> They originate from a moving-average crossover
            concept, but in the STOCK/ETF patterns they are repurposed for different filters.
            The meaning depends on the market type:

            <div className="guide-metric-table" style={{marginTop: '0.75rem'}}>
              <table>
                <thead>
                  <tr><th>Parameter</th><th>FX Patterns</th><th>STOCK / ETF Patterns</th></tr>
                </thead>
                <tbody>
                  <tr>
                    <td><strong>slow_window</strong></td>
                    <td>
                      <em>Slow moving average</em> — the longer lookback (e.g., 20 bars).
                      The price must be above this average.<br />
                      Traditional usage: fast MA crossing above slow MA = momentum.
                    </td>
                    <td>
                      <em>Momentum confirmation</em> — the <strong>shorter</strong> lookback (e.g., STOCK=1, ETF=3).<br />
                      The system checks the last N bars before today and requires <strong>all N</strong> to have positive returns
                      (green days).<br />
                      <strong>Example:</strong> slow_window=3 means "the 3 most recent prior days must ALL
                      have had positive returns." If any of them were negative, the signal does not fire.
                    </td>
                  </tr>
                  <tr>
                    <td><strong>fast_window</strong></td>
                    <td>
                      <em>Fast moving average</em> — the shorter lookback (e.g., 10 bars).
                      The price must be above this average.
                    </td>
                    <td>
                      <em>Breakout + volatility window</em> — the <strong>longer</strong> lookback (e.g., STOCK=5, ETF=20).<br />
                      Two uses:<br />
                      1. <strong>Breakout:</strong> Today's close must exceed the highest close in the prior N bars
                      (an N-bar high breakout).<br />
                      2. <strong>Z-score:</strong> The standard deviation of returns over the last N bars is used to
                      calculate how unusual today's move is.<br />
                      <strong>Example:</strong> fast_window=20 means "price must be at a 20-day high, and
                      z-score is measured over 20 days of return volatility."
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p style={{marginTop: '0.5rem', fontSize: '0.92rem', color: '#616161'}}>
              <strong>Why the naming is counterintuitive:</strong> For STOCK/ETF, "slow" is actually the <em>shorter</em> window
              and "fast" is the <em>longer</em> one. This is an artifact of the codebase reusing the same parameter names
              for a different algorithm. The FX path uses a traditional moving-average crossover; the STOCK/ETF path
              uses momentum confirmation + breakout detection.
            </p>
          </dd>
          <dt>lookback_days</dt>
          <dd>
            <strong>What:</strong> How many days of history to use for computing z-scores and statistics (e.g., 90 days).<br />
            <strong>Why:</strong> Determines "normal" for this symbol. 90 days means the system judges today's move against
            the last ~3 months of behavior.
          </dd>
        </dl>

        <div className="guide-callout">
          <strong>Important:</strong>
          Signals that fire are not automatically traded. They enter the training pipeline first.
          Only signals from TRUSTED patterns can become trade proposals.
        </div>
      </section>

      {/* ─── 4. OUTCOME EVALUATION ─── */}
      <section className="guide-section" id="outcomes">
        <h2>4. Outcome Evaluation (How Training Works)</h2>
        <p>
          Training is <strong>not</strong> teaching an AI model. It's building a track record.
          Every day, the system looks back at old signals and checks what actually happened
          in the market afterward.
        </p>

        <div className="guide-outcome-timeline">
          <div className="guide-timeline-event guide-timeline-event--signal">
            <div className="guide-timeline-day">Day 1 — Signal Fires</div>
            <div className="guide-timeline-title">Pattern detects momentum in AAPL</div>
            <div className="guide-timeline-desc">
              AAPL returned +1.2% today, above the pattern's 0.2% threshold.
              A signal is logged with score = 0.012. No trade happens yet — just a record.
              <br /><strong>AAPL price at signal: $190.00</strong>
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--eval">
            <div className="guide-timeline-day">Day 2 — 1-bar evaluation</div>
            <div className="guide-timeline-title">System checks: what happened 1 bar later?</div>
            <div className="guide-timeline-desc">
              AAPL closed at $190.95 the next day — up +0.5% from $190.00.<br />
              <strong>realized_return = +0.005, hit_flag = true</strong> (exceeded minimum).
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--eval">
            <div className="guide-timeline-day">Day 4 — 3-bar evaluation</div>
            <div className="guide-timeline-title">System checks: what happened 3 bars later?</div>
            <div className="guide-timeline-desc">
              AAPL closed at $192.30 — up +1.2% from $190.00 over 3 days.<br />
              <strong>realized_return = +0.012, hit_flag = true.</strong>
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--eval">
            <div className="guide-timeline-day">Day 6 — 5-bar evaluation</div>
            <div className="guide-timeline-title">System checks: what happened 5 bars later?</div>
            <div className="guide-timeline-desc">
              AAPL closed at $193.99 — up +2.1% from $190.00 over 5 trading days.<br />
              <strong>realized_return = +0.021, hit_flag = true.</strong>
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--eval">
            <div className="guide-timeline-day">Day 11 — 10-bar evaluation</div>
            <div className="guide-timeline-title">System checks: what happened 10 bars later?</div>
            <div className="guide-timeline-desc">
              AAPL closed at $191.71 — up +0.9% from $190.00 over 10 days.<br />
              <strong>realized_return = +0.009, hit_flag = true.</strong>
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--eval">
            <div className="guide-timeline-day">Day 21 — 20-bar evaluation</div>
            <div className="guide-timeline-title">System checks: what happened 20 bars later?</div>
            <div className="guide-timeline-desc">
              Over 20 days, AAPL dropped to $189.43 — down -0.3% from entry at $190.00.<br />
              <strong>realized_return = -0.003, hit_flag = false</strong> (below threshold).
            </div>
          </div>

          <div className="guide-timeline-event guide-timeline-event--result">
            <div className="guide-timeline-day">Ongoing — Metrics accumulate</div>
            <div className="guide-timeline-title">Each evaluation feeds into training metrics</div>
            <div className="guide-timeline-desc">
              After many signals and evaluations, the system has a track record:
              "Out of 40 signals, 31 were hits (77% hit rate) with an average return of +0.81%."
            </div>
          </div>
        </div>

        <h3>The five horizons</h3>
        <p>
          Every signal is evaluated at <strong>5 different time windows</strong> (called "horizons"):
        </p>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Horizon</th><th>Meaning</th><th>What it tells you</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>1 bar</strong></td><td>Next trading day</td><td>Very short-term reaction — did the momentum continue tomorrow?</td></tr>
              <tr><td><strong>3 bars</strong></td><td>3 trading days later</td><td>Short-term follow-through — did the move extend over a few days?</td></tr>
              <tr><td><strong>5 bars</strong></td><td>1 trading week later</td><td>The "standard" holding horizon — the main metric used for scoring.</td></tr>
              <tr><td><strong>10 bars</strong></td><td>2 trading weeks later</td><td>Medium-term — does the pattern have staying power?</td></tr>
              <tr><td><strong>20 bars</strong></td><td>1 trading month later</td><td>Longer-term — was this a meaningful trend or just a blip?</td></tr>
            </tbody>
          </table>
        </div>

        <div className="guide-callout">
          <strong>This is backtesting in production.</strong>
          The system generates signals every day, then evaluates them at multiple time horizons.
          Over weeks and months, this builds a statistically meaningful
          track record for each symbol/pattern combination.
        </div>
      </section>

      {/* ─── 5. TRAINING STAGES ─── */}
      <section className="guide-section" id="training-stages">
        <h2>5. Training Stages</h2>
        <p>
          Every symbol/pattern is assigned a <strong>maturity score</strong> (0–100)
          based on three factors: sample size, outcome coverage, and horizon completeness.
          The score determines the training stage:
        </p>

        <div className="guide-stages">
          <div className="guide-stage guide-stage--insufficient">
            <div className="guide-stage-score">Score 0–24</div>
            <div className="guide-stage-name">INSUFFICIENT</div>
            <div className="guide-stage-desc">
              Not enough data yet. Maybe only 5 signals have been generated.
              The system needs at least 30–40 before it can judge quality. No trading possible.
            </div>
            <span className="guide-stage-arrow">→</span>
          </div>
          <div className="guide-stage guide-stage--warming">
            <div className="guide-stage-score">Score 25–49</div>
            <div className="guide-stage-name">WARMING UP</div>
            <div className="guide-stage-desc">
              Some data exists — maybe 10-20 signals with partial outcome evaluations.
              The system is collecting evidence but it's early days.
            </div>
            <span className="guide-stage-arrow">→</span>
          </div>
          <div className="guide-stage guide-stage--learning">
            <div className="guide-stage-score">Score 50–74</div>
            <div className="guide-stage-name">LEARNING</div>
            <div className="guide-stage-desc">
              Enough data to start judging quality — maybe 25+ signals with outcomes
              across most horizons. Metrics are becoming statistically meaningful.
            </div>
            <span className="guide-stage-arrow">→</span>
          </div>
          <div className="guide-stage guide-stage--confident">
            <div className="guide-stage-score">Score 75–100</div>
            <div className="guide-stage-name">CONFIDENT</div>
            <div className="guide-stage-desc">
              Strong evidence — 40+ signals, outcomes across all 5 horizons.
              If it also passes trust rules, it becomes trade-eligible.
            </div>
          </div>
        </div>

        <h3>What makes up the maturity score?</h3>
        <p>The maturity score (0-100) is calculated from three components:</p>
        <div className="guide-score-breakdown">
          <div className="guide-score-item">
            <div className="guide-score-weight">30%</div>
            <div className="guide-score-name">Sample Size</div>
            <div className="guide-score-explain">
              How many signals have been generated. Needs at least 40 for full marks (30 pts).
            </div>
            <div className="guide-score-example">
              Example: 25 signals → 25/40 × 30 = <strong>18.75 points</strong>
            </div>
          </div>
          <div className="guide-score-item">
            <div className="guide-score-weight">40%</div>
            <div className="guide-score-name">Coverage</div>
            <div className="guide-score-explain">
              What fraction of signals have been evaluated across horizons. 100% means every signal has outcomes for all time windows.
            </div>
            <div className="guide-score-example">
              Example: 80% coverage → 0.80 × 40 = <strong>32.0 points</strong>
            </div>
          </div>
          <div className="guide-score-item">
            <div className="guide-score-weight">30%</div>
            <div className="guide-score-name">Horizons</div>
            <div className="guide-score-explain">
              How many evaluation windows (1, 3, 5, 10, 20 bars) have data. All 5 = full horizon coverage.
            </div>
            <div className="guide-score-example">
              Example: 4 of 5 horizons → 4/5 × 30 = <strong>24.0 points</strong>
            </div>
          </div>
        </div>
        <div className="guide-example">
          <div className="guide-example-title">Full Calculation Example</div>
          <p>
            AUD/USD with FX_MOMENTUM_DAILY: 25 signals, 80% coverage, 4 of 5 horizons.
          </p>
          <p>
            Score = 18.75 + 32.0 + 24.0 = <strong>74.75 → LEARNING stage</strong>
          </p>
          <p>
            To reach CONFIDENT (75+), it needs either more signals (pushing past 30 of 40) or
            the 5th horizon to start populating (which happens after 20 bars pass from the earliest signals).
          </p>
        </div>
      </section>

      {/* ─── 6. TRUST & ELIGIBILITY ─── */}
      <section className="guide-section" id="trust">
        <h2>6. Trust &amp; Eligibility</h2>
        <p>
          Being CONFIDENT (good data quality) is necessary but not sufficient.
          To actually trade, a pattern must also be <strong>TRUSTED</strong> — meaning its
          track record passes three performance gates:
        </p>

        <div className="guide-trust-checklist">
          <div className="guide-trust-item">
            <span className="guide-trust-icon">📏</span>
            <div className="guide-trust-label">Sample Size</div>
            <div className="guide-trust-threshold">&ge; 40 signals</div>
            <div className="guide-trust-explain">
              Enough evaluated signals to be statistically meaningful.
              Below 40, the numbers could be luck — like flipping a coin 5 times
              and getting all heads.
            </div>
          </div>
          <div className="guide-trust-item">
            <span className="guide-trust-icon">🎯</span>
            <div className="guide-trust-label">Hit Rate</div>
            <div className="guide-trust-threshold">&ge; 55%</div>
            <div className="guide-trust-explain">
              More than half of evaluated outcomes must be "hits" —
              the price moved favorably beyond the minimum threshold.
              55% means a meaningful edge over random chance (50%).
            </div>
          </div>
          <div className="guide-trust-item">
            <span className="guide-trust-icon">💰</span>
            <div className="guide-trust-label">Avg Return</div>
            <div className="guide-trust-threshold">&ge; 0.05%</div>
            <div className="guide-trust-explain">
              The average realized return across all outcomes must be positive
              and above 0.0005 (0.05%). This ensures actual profitability,
              not just accuracy.
            </div>
          </div>
        </div>

        <h3>Trust labels</h3>
        <dl className="guide-kv guide-kv--wide">
          <dt>TRUSTED</dt>
          <dd>All three gates passed. The pattern CAN generate trade proposals. This is the definitive answer — once trusted, signals from this pattern are eligible for trading.</dd>
          <dt>WATCH</dt>
          <dd>Close but not there yet. One or two gates are nearly met. The system is monitoring progress. No trading allowed — but it could flip to TRUSTED with a few more good outcomes.</dd>
          <dt>UNTRUSTED</dt>
          <dd>Fails trust criteria significantly. Not enough evidence or poor performance. No trading. The pattern needs more time or may simply not be reliable for this symbol.</dd>
        </dl>

        <div className="guide-callout--warn guide-callout">
          <strong>CONFIDENT ≠ TRUSTED</strong>
          A symbol can be CONFIDENT (score 85, lots of data) but NOT TRUSTED (e.g., hit rate is 50%,
          below the 55% threshold — plenty of data but poor accuracy). Conversely, a symbol can be TRUSTED
          before reaching CONFIDENT if its returns are strong on existing data.
          <strong>The trust label is the definitive gate for trading.</strong>
        </div>
      </section>

      {/* ─── 7. WHAT IS HIT RATE? ─── */}
      <section className="guide-section" id="hit-rate">
        <h2>7. What Is Hit Rate?</h2>
        <p>
          Hit rate answers: <strong>"When this pattern said 'go,' how often did the price
          actually move favorably?"</strong>
        </p>

        <div style={{ margin: '1.5rem 0' }}>
          <svg viewBox="0 0 500 120" style={{ width: '100%', maxWidth: 500 }}>
            <rect x="30" y="20" width="440" height="40" rx="8" fill="#e8eaf6" />
            <rect x="30" y="20" width="330" height="40" rx="8" fill="#66bb6a" />
            <text x="420" y="45" textAnchor="middle" fontSize="13" fill="#666" fontWeight="600">Miss</text>
            <text x="195" y="45" textAnchor="middle" fontSize="13" fill="#fff" fontWeight="600">Hits (30 of 40)</text>
            <text x="250" y="90" textAnchor="middle" fontSize="20" fill="#2e7d32" fontWeight="700">75% Hit Rate</text>
            <line x1="272" y1="15" x2="272" y2="65" stroke="#c62828" strokeWidth="2" strokeDasharray="4" />
            <text x="272" y="80" textAnchor="middle" fontSize="11" fill="#c62828">55% threshold</text>
          </svg>
        </div>

        <h3>How it's calculated</h3>
        <p className="guide-formula">
          Hit Rate = HIT_COUNT ÷ SUCCESS_COUNT
        </p>
        <dl className="guide-kv guide-kv--wide">
          <dt>SUCCESS_COUNT</dt>
          <dd>The number of outcomes that were successfully evaluated — the system was able to check what happened after the signal fired. For example, if 40 signals have been generated and all 40 have a known outcome at the 5-bar horizon, SUCCESS_COUNT = 40.</dd>
          <dt>HIT_COUNT</dt>
          <dd>Of those, how many had the price move above the minimum threshold in the right direction. If 30 out of 40 outcomes showed a positive return exceeding the pattern's minimum, HIT_COUNT = 30.</dd>
        </dl>

        <div className="guide-example">
          <div className="guide-example-title">Detailed Example</div>
          <p>
            AUD/USD has been tracked for 3 months. In that time, the FX_MOMENTUM_DAILY pattern fired 40 times.
            The system evaluated what happened 5 bars (1 week) after each signal:
          </p>
          <ul>
            <li>Signal #1: AUD went up +0.5% → <strong>HIT</strong> (above threshold)</li>
            <li>Signal #2: AUD went down -0.2% → <strong>MISS</strong></li>
            <li>Signal #3: AUD went up +1.1% → <strong>HIT</strong></li>
            <li>... and so on for all 40 signals ...</li>
          </ul>
          <p>
            Final count: 30 hits out of 40 total evaluations.<br />
            <strong>Hit Rate = 30 / 40 = 0.75 (75%)</strong>
          </p>
          <p>
            The system requires at least 55%. At 75%, AUD/USD passes this gate comfortably —
            meaning 3 out of 4 times the pattern fired, the price moved favorably.
          </p>
        </div>
      </section>

      {/* ─── 8. WHAT IS AVG RETURN? ─── */}
      <section className="guide-section" id="avg-return">
        <h2>8. What Is Avg Return?</h2>
        <p>
          Avg return answers: <strong>"On average, how much money did this pattern
          make (or lose) per signal?"</strong>
        </p>
        <p>
          While hit rate measures <em>accuracy</em> (how often), avg return measures
          <em> profitability</em> (how much). Both are needed — a pattern could be right
          60% of the time but still lose money if the losses are bigger than the wins.
        </p>

        <div style={{ margin: '1.5rem 0' }}>
          <svg viewBox="0 0 500 160" style={{ width: '100%', maxWidth: 500 }}>
            <line x1="50" y1="80" x2="460" y2="80" stroke="#ccc" strokeWidth="1" />
            <text x="50" y="100" fontSize="10" fill="#999">-1.0%</text>
            <text x="250" y="100" fontSize="10" fill="#999" textAnchor="middle">0%</text>
            <text x="460" y="100" fontSize="10" fill="#999" textAnchor="end">+2.0%</text>
            <line x1="250" y1="25" x2="250" y2="85" stroke="#999" strokeWidth="1" strokeDasharray="3" />
            {[
              { x: 285, h: 25, color: '#66bb6a' },
              { x: 300, h: 40, color: '#66bb6a' },
              { x: 315, h: 15, color: '#66bb6a' },
              { x: 330, h: 50, color: '#66bb6a' },
              { x: 345, h: 30, color: '#66bb6a' },
              { x: 220, h: 20, color: '#ef5350' },
              { x: 205, h: 35, color: '#ef5350' },
              { x: 360, h: 20, color: '#66bb6a' },
              { x: 375, h: 10, color: '#66bb6a' },
              { x: 235, h: 10, color: '#ef5350' },
            ].map((bar, i) => (
              <rect
                key={i}
                x={bar.x}
                y={bar.color === '#66bb6a' ? 80 - bar.h : 80}
                width="12"
                height={bar.h}
                rx="2"
                fill={bar.color}
                opacity="0.8"
              />
            ))}
            <line x1="320" y1="20" x2="320" y2="85" stroke="#1a237e" strokeWidth="2" />
            <text x="320" y="15" textAnchor="middle" fontSize="12" fill="#1a237e" fontWeight="700">Avg: +0.81%</text>
            <line x1="255" y1="20" x2="255" y2="85" stroke="#c62828" strokeWidth="1.5" strokeDasharray="4" />
            <text x="255" y="120" textAnchor="middle" fontSize="10" fill="#c62828">0.05% threshold</text>
            <text x="350" y="145" textAnchor="middle" fontSize="11" fill="#2e7d32" fontWeight="600">Profitable outcomes</text>
            <text x="200" y="145" textAnchor="middle" fontSize="11" fill="#c62828" fontWeight="600">Losing outcomes</text>
          </svg>
        </div>

        <h3>How it's calculated</h3>
        <p className="guide-formula">
          Avg Return = sum of all realized_return values ÷ number of evaluated outcomes
        </p>

        <div className="guide-example">
          <div className="guide-example-title">Detailed Example</div>
          <p>
            AUD/USD generated 40 signals. Here are the 5-bar returns for 8 of them:
          </p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Signal</th><th>5-bar return</th><th>Hit?</th></tr>
              </thead>
              <tbody>
                <tr><td>#1</td><td className="guide-positive">+1.20%</td><td>Yes</td></tr>
                <tr><td>#2</td><td className="guide-positive">+0.50%</td><td>Yes</td></tr>
                <tr><td>#3</td><td className="guide-negative">-0.30%</td><td>No</td></tr>
                <tr><td>#4</td><td className="guide-positive">+0.80%</td><td>Yes</td></tr>
                <tr><td>#5</td><td className="guide-positive">+2.10%</td><td>Yes</td></tr>
                <tr><td>#6</td><td className="guide-negative">-0.50%</td><td>No</td></tr>
                <tr><td>#7</td><td className="guide-positive">+0.30%</td><td>Yes</td></tr>
                <tr><td>#8</td><td className="guide-positive">+1.50%</td><td>Yes</td></tr>
              </tbody>
            </table>
          </div>
          <p>
            Average across all 40 outcomes: <strong>+0.81%</strong> per signal.
          </p>
          <p>
            A value of 0.0081 (0.81%) is well above the 0.0005 (0.05%) threshold.
            Even though some signals lost money (-0.30%, -0.50%), the average is solidly positive.
          </p>
        </div>
      </section>

      {/* ─── 9. FROM TRUST TO TRADING ─── */}
      <section className="guide-section" id="trading">
        <h2>9. From Trust to Trading</h2>
        <p>
          Even after a pattern earns TRUSTED status, several more gates must be passed
          before an actual trade happens:
        </p>

        <div className="guide-pipeline-flow">
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box" style={{ borderColor: '#66bb6a' }}>
              <span className="guide-pipeline-icon">✅</span>
              <div className="guide-pipeline-label">TRUSTED Signal</div>
              <div className="guide-pipeline-desc">Pattern passed all trust gates</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">🚦</span>
              <div className="guide-pipeline-label">Risk Gate</div>
              <div className="guide-pipeline-desc">Is the portfolio safe? Is entry allowed?</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">📦</span>
              <div className="guide-pipeline-label">Capacity Check</div>
              <div className="guide-pipeline-desc">Does the portfolio have open slots?</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box">
              <span className="guide-pipeline-icon">📝</span>
              <div className="guide-pipeline-label">Proposal</div>
              <div className="guide-pipeline-desc">Order proposed &amp; validated</div>
            </div>
          </div>
          <div className="guide-pipeline-arrow">→</div>
          <div className="guide-pipeline-step">
            <div className="guide-pipeline-step-box" style={{ borderColor: '#66bb6a', background: '#e8f5e9' }}>
              <span className="guide-pipeline-icon">💹</span>
              <div className="guide-pipeline-label">Trade Executed</div>
              <div className="guide-pipeline-desc">Position opened in portfolio</div>
            </div>
          </div>
        </div>

        <p>
          The <strong>proposal funnel</strong> shows this narrowing effect. If you see
          many signals but few trades, it's usually because:
        </p>
        <ul>
          <li><strong>Most patterns are still in WATCH status</strong> — they haven't earned trust yet, so their signals can't become proposals.</li>
          <li><strong>The portfolio is fully saturated</strong> — all position slots are in use. New trades can't open until existing ones close.</li>
          <li><strong>The risk gate is in CAUTION or STOPPED mode</strong> — the portfolio's drawdown exceeded a safety threshold, blocking new entries.</li>
          <li><strong>Duplicate position</strong> — the portfolio already holds this symbol, so a new entry is rejected.</li>
        </ul>

        <div className="guide-example">
          <div className="guide-example-title">Example: A typical day's funnel</div>
          <p>
            Today's pipeline generated <strong>15 signals</strong> across all symbols.
            Of those, <strong>3</strong> came from TRUSTED patterns (the rest are still training).
            Of those 3, <strong>2</strong> passed the risk gate (the portfolio's gate is SAFE).
            Of those 2, <strong>1</strong> passed the capacity check (1 slot was available).
            Result: <strong>1 trade executed</strong> out of 15 signals. That's normal.
          </p>
        </div>
      </section>

      {/* ─── 10. WHAT ARE PATTERNS? ─── */}
      <section className="guide-section" id="patterns">
        <h2>10. What Are Patterns?</h2>
        <p>
          A <strong>pattern</strong> is a named signal strategy with specific parameters.
          Each pattern defines what the system looks for in market data. MIP can run
          multiple patterns simultaneously, each targeting different market types and
          time scales.
        </p>

        <div className="guide-example">
          <div className="guide-example-title">Pattern Examples</div>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Pattern</th><th>Market</th><th>fast_window</th><th>slow_window</th><th>What It Actually Requires</th><th>Min Return</th><th>Min Z-Score</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>FX_MOMENTUM_DAILY</strong></td>
                  <td>FX</td>
                  <td>10</td>
                  <td>20</td>
                  <td>Price above 10-bar MA <em>and</em> 20-bar MA (traditional crossover)</td>
                  <td>0.1%</td>
                  <td>0 (any)</td>
                </tr>
                <tr>
                  <td><strong>STOCK_MOMENTUM_FAST</strong></td>
                  <td>STOCK</td>
                  <td>5</td>
                  <td>1</td>
                  <td>1 prior green day + 5-day high breakout + z-score ≥ 1.0</td>
                  <td>0.2%</td>
                  <td>1.0</td>
                </tr>
                <tr>
                  <td><strong>ETF_MOMENTUM_DAILY</strong></td>
                  <td>ETF</td>
                  <td>20</td>
                  <td>3</td>
                  <td>3 consecutive green days + 20-day high breakout + z-score ≥ 1.0</td>
                  <td>0.2%</td>
                  <td>1.0</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p>
            <strong>FX_MOMENTUM_DAILY</strong> uses the traditional moving-average approach (less selective: z-score = 0, any positive move above 0.1% fires).
          </p>
          <p>
            <strong>STOCK_MOMENTUM_FAST</strong> requires a 5-day high breakout with at least 1 prior green day — achievable and fires regularly.
          </p>
          <p>
            <strong>ETF_MOMENTUM_DAILY</strong> is paradoxically the <em>most demanding</em> pattern: it requires
            3 consecutive green days AND a 20-day high breakout AND z-score ≥ 1.0. This is extremely selective,
            which is why ETF signals fire less frequently despite targeting less volatile instruments.
          </p>
        </div>

        <p>
          Each symbol is evaluated <em>per pattern</em>. So AAPL might be CONFIDENT
          under STOCK_MOMENTUM_FAST but INSUFFICIENT under a different pattern. Trust
          is also earned per pattern — the system judges each strategy independently.
        </p>

        <div className="guide-callout">
          <strong>Patterns don't change automatically.</strong>
          The AI narratives in the Cockpit describe pattern behavior but never change
          pattern parameters. Only the system operator can modify pattern definitions.
          The AI is strictly observational — it reports, it doesn't act.
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════════ */}
      {/*                PART 2 — PAGE-BY-PAGE GUIDE                    */}
      {/* ═══════════════════════════════════════════════════════════════ */}
      <div className="guide-part-header">Part 2 — Page-by-Page Guide</div>

      {/* ─── 11. HOME ─── */}
      <section className="guide-section" id="page-home">
        <h2>11. Home</h2>
        <p className="guide-page-purpose">
          Your landing page. A quick overview of system health and shortcuts to the most-used pages.
        </p>

        <h3>What you see on this page</h3>

        <div className="guide-page-section">
          <h4>Hero Banner</h4>
          <p>
            The title "Market Intelligence Platform" with the tagline
            "Daily-bar research • outcomes-based learning • explainable suggestions."
            This is decorative — it reminds you of MIP's purpose.
          </p>
        </div>

        <div className="guide-page-section">
          <h4>Quick Actions</h4>
          <p>Five shortcut cards that take you directly to key pages:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Card</th><th>Where it goes</th><th>What it shows</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>View Portfolios</strong></td><td>/portfolios</td><td>All portfolios — positions, trades, episodes</td></tr>
                <tr><td><strong>Default Portfolio</strong></td><td>/portfolios/1</td><td>Quick link to your primary portfolio</td></tr>
                <tr><td><strong>Open Cockpit</strong></td><td>/cockpit</td><td>AI narratives, portfolio status, training</td></tr>
                <tr><td><strong>Open Training Status</strong></td><td>/training</td><td>Maturity by symbol and pattern</td></tr>
                <tr><td><strong>Open Suggestions</strong></td><td>/suggestions</td><td>Ranked candidates from outcome history</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="guide-page-section">
          <h4>System at a Glance — Three metric cards</h4>
          <dl className="guide-kv guide-kv--wide">
            <dt>Last pipeline run</dt>
            <dd>
              Shows how long ago the daily pipeline finished, plus its status (SUCCESS / FAILED / RUNNING).
              <br /><strong>Example:</strong> "2 hours ago" with a green "SUCCESS" badge means the pipeline
              ran 2 hours ago and completed normally.
            </dd>
            <dt>New evaluations since last run</dt>
            <dd>
              How many new outcome evaluations have been calculated since the last pipeline run.
              <br /><strong>Example:</strong> "+12" means 12 new outcomes were computed. This number grows
              as time passes and more horizons become evaluable. If it says "0", no new outcomes are ready yet.
            </dd>
            <dt>Latest digest (as-of)</dt>
            <dd>
              When the most recent AI digest was generated.
              <br /><strong>Example:</strong> "3 hours ago" means the digest covers data up to 3 hours ago.
              "No digest yet" means no digest exists (run the pipeline first).
            </dd>
          </dl>
        </div>
      </section>

      {/* ─── 12. COCKPIT ─── */}
      <section className="guide-section" id="page-cockpit">
        <h2>12. Cockpit (Daily Dashboard)</h2>
        <p className="guide-page-purpose">
          Your daily command center. Shows AI-generated narratives, training status, signal candidates,
          and upcoming symbols — all in one view. This is the page you should check first every morning.
        </p>

        <h3>Portfolio Picker (top-right dropdown)</h3>
        <p>
          Select which portfolio's intelligence to display. The global digest (system-wide) always shows.
          The portfolio digest changes based on your selection.
        </p>

        <div className="guide-page-section">
          <h4>Row 1: AI-Generated Digests (two cards side by side)</h4>

          <h5>Left Card — System Overview (Global Digest)</h5>
          <p>
            This card shows an AI-generated narrative covering the <strong>entire system</strong> — all portfolios, all symbols, all patterns.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Headline</dt>
            <dd>A one-sentence summary of today's most important development. Written by Snowflake Cortex AI.
              <br /><strong>Example:</strong> "3 new trusted symbols unlocked, pipeline healthy, portfolio capacity at 80%."</dd>
            <dt>Cortex AI / Deterministic badge</dt>
            <dd><strong>"Cortex AI"</strong> means the narrative was generated by the Snowflake Cortex LLM, grounded in today's snapshot facts.
              <strong>"Deterministic"</strong> means the AI was unavailable and the system fell back to a template-based summary.</dd>
            <dt>Fresh / Stale badge</dt>
            <dd><strong>"Fresh (45m ago)"</strong> — digest is recent (under 2 hours old).
              <strong>"Stale (3h ago)"</strong> — digest is older than 2 hours. Run the pipeline to refresh.</dd>
            <dt>Detector Pills</dt>
            <dd>Colored pills showing which "interest detectors" fired today. These are deterministic rules like "Gate status changed" or "New trusted pattern."
              <br />Colors: <strong style={{color:'#388e3c'}}>green</strong> = low severity (informational),
              <strong style={{color:'#f57c00'}}> orange</strong> = medium (noteworthy),
              <strong style={{color:'#c62828'}}> red</strong> = high (urgent).</dd>
            <dt>What Changed</dt>
            <dd>Bullets describing what's different from yesterday. Example: "AUD/USD moved from LEARNING to CONFIDENT stage."</dd>
            <dt>What Matters</dt>
            <dd>Bullets explaining the most important observations and their implications. Example: "Only 2 of 12 momentum signals passed the z-score gate (observed &lt; 1.0), suggesting the market is quiet — expect fewer proposals."</dd>
            <dt>Waiting For</dt>
            <dd>Bullets describing upcoming triggers or thresholds. Example: "EUR/USD needs 5 more evaluated outcomes to reach CONFIDENT (35 of 40 threshold)."</dd>
          </dl>

          <h5>Right Card — Portfolio Intelligence</h5>
          <p>
            Same structure as the global card, but scoped to your selected portfolio. Shows what changed for that portfolio's positions, risk gate, and capacity.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Episode Badge (purple)</dt>
            <dd>
              Shows which episode the portfolio is in. Example: <strong>"Episode 3 (of 3)"</strong> means this is the 3rd lifecycle period.
              Hover over it to see when the episode started. All performance numbers in the narrative are scoped to this episode — they
              reflect the current cycle, not lifetime totals.
              <br /><strong>Why this matters:</strong> After a crystallization event or profile change, a new episode starts with a fresh
              cost basis. The AI narrative knows this and reports performance relative to the current episode only.
            </dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Row 2: Global Training Digest</h4>
          <p>
            An AI narrative focused on the training pipeline across all symbols. Shows journey steps (visual arrows),
            what changed in training, what matters for upcoming trust decisions, and what the system is waiting for.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Journey Steps</dt>
            <dd>A visual path like: <code>INSUFFICIENT → WARMING_UP → LEARNING → &gt;&gt; CONFIDENT</code>.
              The ">>" marks where most symbols currently are in the training process.</dd>
            <dt>Show training snapshot facts</dt>
            <dd>A toggle that reveals the raw JSON snapshot — the exact data the AI used to write the narrative. Useful for verifying that the AI isn't making things up.</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Row 3: Today's Signal Candidates &amp; Upcoming Symbols (two cards)</h4>

          <h5>Left Card — Today's Signal Candidates</h5>
          <p>
            The top 6 symbols with eligible signals today, ranked by maturity and outcome history.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Rank (#1–#6)</dt>
            <dd>Position in the ranking. #1 has the highest combined maturity + return score.</dd>
            <dt>Symbol</dt>
            <dd>The asset being tracked (e.g., AAPL, EUR/USD).</dd>
            <dt>Maturity Stage pill</dt>
            <dd>Color-coded badge showing INSUFFICIENT / WARMING_UP / LEARNING / CONFIDENT.</dd>
            <dt>Progress bar</dt>
            <dd>Visual bar from 0–100 showing the maturity score. A score of 75 fills 75% of the bar.</dd>
            <dt>"Why" text</dt>
            <dd>A deterministic explanation of why this symbol appears here. Example: "Meets minimum recs ≥ 10, horizon coverage 100%, strong 5-bar return."</dd>
          </dl>

          <h5>Right Card — Upcoming Symbols</h5>
          <p>
            Symbols closest to advancing to the next training stage or becoming trade-eligible.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Near-Miss Symbols</dt>
            <dd>Up to 6 symbols that are almost at the next stage threshold. Shows the gap in points.
              <br /><strong>Example:</strong> "Score: 72/100, Gap to CONFIDENT: 3 pts" means this symbol needs just 3 more points to reach CONFIDENT.</dd>
            <dt>Trade-Ready (CONFIDENT)</dt>
            <dd>Up to 4 symbols that are already CONFIDENT. These are the strongest candidates that may already be generating proposals if trusted.</dd>
          </dl>
        </div>
      </section>

      {/* ─── 13. PORTFOLIO ─── */}
      <section className="guide-section" id="page-portfolio">
        <h2>13. Portfolio</h2>
        <p className="guide-page-purpose">
          Deep dive into a single portfolio — its money, positions, trades, risk status, and
          historical performance. If the Cockpit is your morning summary, the Portfolio page is
          where you go for the full picture.
        </p>

        <h3>Portfolio List View (Control Tower)</h3>
        <p>When you navigate to /portfolios without selecting one, you see the <strong>Control Tower</strong> — a table showing all portfolios at a glance.</p>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Column</th><th>What it means</th><th>Example</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>Name</strong></td><td>Portfolio name (clickable link)</td><td>Main FX Portfolio</td></tr>
              <tr><td><strong>ID</strong></td><td>Unique identifier</td><td>1</td></tr>
              <tr>
                <td><strong>Gate</strong></td>
                <td>Risk regime. A traffic light indicator:
                  <br /><span style={{color:'#2e7d32'}}>SAFE</span> = entries allowed
                  <br /><span style={{color:'#f57c00'}}>CAUTION</span> = approaching threshold
                  <br /><span style={{color:'#c62828'}}>STOPPED</span> = entries blocked</td>
                <td><span style={{color:'#2e7d32'}}>● SAFE</span></td>
              </tr>
              <tr>
                <td><strong>Health</strong></td>
                <td>Data freshness:
                  <br /><span style={{color:'#2e7d32'}}>OK</span> = recent run
                  <br /><span style={{color:'#f57c00'}}>STALE</span> = older than 24h
                  <br /><span style={{color:'#c62828'}}>BROKEN</span> = very old or failed</td>
                <td><span style={{color:'#2e7d32'}}>● OK</span></td>
              </tr>
              <tr><td><strong>Equity</strong></td><td>Latest total equity (cash + position value)</td><td>€102,450</td></tr>
              <tr><td><strong>Paid Out</strong></td><td>Cumulative profits withdrawn across all episodes</td><td>€5,200</td></tr>
              <tr><td><strong>Active Episode</strong></td><td>Current episode ID and start date</td><td>#3 since 2026-01-15</td></tr>
              <tr><td><strong>Status</strong></td><td>ACTIVE or CLOSED</td><td>ACTIVE</td></tr>
            </tbody>
          </table>
        </div>

        <h3>Portfolio Detail View</h3>
        <p>Click a portfolio to see the full detail page. Here's every section:</p>

        <div className="guide-page-section">
          <h4>Freshness Header</h4>
          <dl className="guide-kv guide-kv--wide">
            <dt>CURRENT / PENDING UPDATE</dt>
            <dd><strong>CURRENT</strong> = portfolio is simulated up to the latest market date. <strong>PENDING UPDATE</strong> = new market data exists but the pipeline hasn't run yet. The header also shows: "Simulated through 2026-02-07 · Pipeline ran at 14:30".</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Active Period Dashboard (Mini Charts)</h4>
          <p>Four small charts showing the current episode's performance at a glance:</p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Equity Chart</dt>
            <dd>Shows total portfolio value over time during this episode. The line goes up when positions gain value or profitable trades close, and down when positions lose value.
              <br /><strong>Example:</strong> Starting at €100,000, the line might rise to €102,450 over 3 weeks.</dd>
            <dt>Drawdown Chart</dt>
            <dd>Shows the percentage drop from the episode's peak equity. A drawdown of -3% means the portfolio has fallen 3% from its highest point.
              <br /><strong>Why it matters:</strong> If drawdown exceeds the threshold (e.g., -5%), the risk gate switches to CAUTION or STOPPED.</dd>
            <dt>Trades per Day</dt>
            <dd>Bar chart showing how many trades were executed each day. Most days will show 0-3 trades. A spike might indicate many positions closing at once.</dd>
            <dt>Regime per Day</dt>
            <dd>Shows the risk regime (NORMAL / CAUTION / DEFENSIVE) for each day. Helps you see when the portfolio was in a restricted state.</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>KPI Cards (Header)</h4>
          <p>Eight key numbers about the portfolio's lifetime performance:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>KPI</th><th>What it means</th><th>How it's calculated</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Starting Cash</strong></td><td>Initial capital when portfolio was created</td><td>Set when creating the portfolio</td><td>€100,000</td></tr>
                <tr><td><strong>Current Cash</strong></td><td>Cash available right now (not invested)</td><td>Starting cash + profits − invested amount</td><td>€87,234.50</td></tr>
                <tr><td><strong>Final Equity</strong></td><td>Total value: cash + all position values</td><td>Cash + sum(quantity × current_price) for each position</td><td>€102,450</td></tr>
                <tr><td><strong>Total Return</strong></td><td>Overall profit/loss as a percentage</td><td>(Final Equity − Starting Cash) / Starting Cash × 100</td><td>+2.45%</td></tr>
                <tr><td><strong>Max Drawdown</strong></td><td>Worst peak-to-trough decline ever</td><td>Largest % drop from any high to subsequent low</td><td>-4.20%</td></tr>
                <tr><td><strong>Win Days</strong></td><td>Days where equity increased</td><td>Count of days where end-of-day equity &gt; previous day</td><td>42</td></tr>
                <tr><td><strong>Loss Days</strong></td><td>Days where equity decreased</td><td>Count of days where end-of-day equity &lt; previous day</td><td>18</td></tr>
                <tr><td><strong>Status</strong></td><td>ACTIVE or CLOSED</td><td>Set by the system</td><td>ACTIVE</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="guide-page-section">
          <h4>Cash &amp; Exposure Card</h4>
          <dl className="guide-kv guide-kv--wide">
            <dt>Cash</dt>
            <dd>Money not currently invested. Available for new positions. Example: €87,234.50</dd>
            <dt>Exposure</dt>
            <dd>Total value of all open positions (quantity × current price). Example: €15,215.50</dd>
            <dt>Total Equity</dt>
            <dd>Cash + Exposure = your total portfolio value. Example: €87,234.50 + €15,215.50 = €102,450.00</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Open Positions Table</h4>
          <p>Current holdings the portfolio has right now:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Column</th><th>Meaning</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Symbol</strong></td><td>Which asset (stock, FX pair)</td><td>AAPL</td></tr>
                <tr><td><strong>Side</strong></td><td>BUY (long) or SELL (short)</td><td>BUY</td></tr>
                <tr><td><strong>Quantity</strong></td><td>How many shares/units held</td><td>50</td></tr>
                <tr><td><strong>Cost Basis</strong></td><td>Average price paid when entering</td><td>$190.00</td></tr>
                <tr><td><strong>Hold Until (bar)</strong></td><td>Bar index when this position is scheduled to close</td><td>1245</td></tr>
                <tr><td><strong>Hold Until (date)</strong></td><td>Calendar date when the position should close</td><td>2026-02-15</td></tr>
              </tbody>
            </table>
          </div>
          <div className="guide-callout">
            <strong>Positions are sorted by "Hold Until" (soonest first).</strong> This lets you see at a glance which positions are about to close.
          </div>
        </div>

        <div className="guide-page-section">
          <h4>Recent Trades Table</h4>
          <p>Execution history — every buy and sell the portfolio has made:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Column</th><th>Meaning</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Symbol</strong></td><td>Which asset was traded</td><td>AUD/USD</td></tr>
                <tr><td><strong>Side</strong></td><td>BUY (opening long) or SELL (closing)</td><td>BUY</td></tr>
                <tr><td><strong>Quantity</strong></td><td>How many units traded</td><td>10,000</td></tr>
                <tr><td><strong>Price</strong></td><td>Execution price</td><td>0.6520</td></tr>
                <tr><td><strong>Notional</strong></td><td>Total value of the trade (Price × Quantity)</td><td>$6,520.00</td></tr>
              </tbody>
            </table>
          </div>
          <p>
            Use the <strong>Lookback</strong> dropdown (1 day, 7 days, 30 days, All) to control how far back
            the trade history goes. The "total" count shows how many trades exist in the selected window.
          </p>
        </div>

        <div className="guide-page-section">
          <h4>Risk Gate Panel</h4>
          <p>The risk gate protects the portfolio from taking too much risk. It has three states:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>State</th><th>Icon</th><th>What it means</th><th>What happens</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>NORMAL</strong></td><td>✅</td><td>Portfolio is within safe limits</td><td>New entries AND exits allowed</td></tr>
                <tr><td><strong>CAUTION</strong></td><td>⚠️</td><td>Drawdown is approaching the threshold</td><td>Entries may still be allowed but the system is watching closely</td></tr>
                <tr><td><strong>DEFENSIVE</strong></td><td>🛑</td><td>Drawdown has breached the safety threshold</td><td>New entries BLOCKED. Only exits (closing positions) allowed.</td></tr>
              </tbody>
            </table>
          </div>
          <dl className="guide-kv guide-kv--wide">
            <dt>Reason Text</dt>
            <dd>Explains why the gate is in its current state. Example: "Episode drawdown at 4.2% (threshold: 5.0%). Approaching risk limit."</dd>
            <dt>What to do now</dt>
            <dd>Actionable guidance. Example: "Wait for existing positions to close and drawdown to recover before new entries are opened."</dd>
            <dt>Risk Strategy Rules</dt>
            <dd>The specific rules being enforced, e.g., "Episode drawdown stop: -5%", "Max concurrent positions: 10".</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Proposer Diagnostics</h4>
          <p>Why proposals may be zero or low — technical details about the proposal engine:</p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Raw Signals (latest bar)</dt>
            <dd>Total signals detected today across all patterns. Example: 15. This is before any filtering.</dd>
            <dt>Trusted Signals</dt>
            <dd>Of those raw signals, how many came from TRUSTED patterns. Example: 3 (only these can become proposals).</dd>
            <dt>Trusted Patterns</dt>
            <dd>How many distinct patterns currently have TRUSTED status. Example: 2.</dd>
            <dt>Rec TS = Bar TS?</dt>
            <dd>Whether the recommendation timestamp matches today's bar date. "Yes" = everything is current. "No" = data may be stale.</dd>
            <dt>Proposals Inserted</dt>
            <dd>How many proposals the engine actually created. Example: 1 (after capacity and duplicate checks).</dd>
            <dt>Reason for Zero</dt>
            <dd>If zero proposals: the specific reason, e.g., "NO_TRUSTED_CANDIDATES" or "ENTRIES_BLOCKED".</dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Cumulative Performance Section</h4>
          <p>Your investment journey across all episodes:</p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Total Paid Out</dt>
            <dd>Profits withdrawn at episode ends. When an episode closes profitably, the gains are "paid out."
              <br /><strong>Example:</strong> €5,200 means the portfolio has withdrawn €5,200 in profits over its lifetime.</dd>
            <dt>Total Realized P&amp;L</dt>
            <dd>Cumulative profit/loss across all episodes. Green = overall profit, Red = overall loss.
              <br /><strong>Example:</strong> €3,850 means the portfolio has earned €3,850 net across all closed trades.</dd>
            <dt>Episodes</dt>
            <dd>How many "generations" the portfolio has been through. Each episode starts with fresh capital and a clean slate.
              <br /><strong>Example:</strong> 3 episodes means the portfolio has been reset/restarted twice.</dd>
            <dt>Cumulative Growth Chart (line chart)</dt>
            <dd>
              Two lines over time:
              <br /><span style={{color:'#2e7d32'}}>Green line (Paid Out)</span> — cumulative profits withdrawn.
              <br /><span style={{color:'#1565c0'}}>Blue line (Realized P&amp;L)</span> — cumulative profit/loss.
              <br />The X-axis is time (dates), Y-axis is amount in euros.
            </dd>
            <dt>P&amp;L by Episode (bar chart)</dt>
            <dd>
              One bar per episode showing its profit or loss.
              <br /><span style={{color:'#2e7d32'}}>Green bars</span> = profitable episodes.
              <span style={{color:'#c62828'}}> Red bars</span> = losing episodes.
              <span style={{color:'#1565c0'}}> Blue bar</span> = the currently active episode.
              <br />Click a bar to scroll down to that episode's detail card.
            </dd>
          </dl>
        </div>

        <div className="guide-page-section">
          <h4>Episodes Section</h4>
          <p>
            Expandable cards for each episode (generation) of the portfolio. Each card shows equity curves,
            drawdown charts, trade counts, and risk regime for that specific episode period.
            The active episode is highlighted.
          </p>
        </div>
      </section>

      {/* ─── 14. PORTFOLIO MANAGEMENT ─── */}
      <section className="guide-section" id="page-manage">
        <h2>14. Portfolio Management</h2>
        <p className="guide-page-purpose">
          Create and configure portfolios, manage risk profiles, deposit/withdraw cash, view
          lifecycle history, and generate AI-powered portfolio stories. This is the operational
          hub where you set up and maintain your portfolios.
        </p>

        <div className="guide-callout--warn guide-callout">
          <strong>Pipeline Lock</strong>
          When the daily pipeline is actively running, <strong>all editing is disabled</strong> on this page.
          A yellow warning banner appears at the top: "Pipeline is currently running — editing is
          disabled until the run completes." This prevents changes from interfering with an active
          simulation. Buttons automatically re-enable once the pipeline finishes (the page polls
          every 15 seconds). You can still browse data and read-only tabs while waiting.
        </div>

        <h3>Tabs</h3>
        <p>The page is organized into four tabs:</p>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Tab</th><th>Purpose</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>Portfolios</strong></td><td>Create/edit portfolios, deposit/withdraw cash, attach risk profiles</td></tr>
              <tr><td><strong>Profiles</strong></td><td>Create/edit risk profiles with position limits, drawdown stops, and crystallization settings</td></tr>
              <tr><td><strong>Lifecycle Timeline</strong></td><td>Visual history of every lifecycle event (charts + timeline) for a selected portfolio</td></tr>
              <tr><td><strong>Portfolio Story</strong></td><td>AI-generated narrative summarizing the portfolio's complete journey</td></tr>
            </tbody>
          </table>
        </div>

        <div className="guide-page-section">
          <h4>Tab 1: Portfolios</h4>
          <p>
            Shows a table of all portfolios with key metrics. Each row displays the portfolio's
            ID, name, assigned risk profile, starting cash, final equity, total return, and status.
          </p>

          <h5>Actions on each portfolio row</h5>
          <dl className="guide-kv guide-kv--wide">
            <dt>Edit</dt>
            <dd>Update the portfolio name, currency, or notes. <strong>Starting cash cannot be changed after creation</strong> — use the Cash button for deposits/withdrawals instead.</dd>
            <dt>Cash</dt>
            <dd>
              Opens the <strong>Cash Event</strong> dialog where you register a deposit or withdrawal.
              <br /><strong>Deposit:</strong> Adds money to the portfolio. Increases cash and equity by the deposited amount.
              <br /><strong>Withdraw:</strong> Removes money from the portfolio. You can only withdraw up to the current cash balance.
              <br /><strong>Important:</strong> Your lifetime P&amp;L tracking stays intact. The system adjusts the cost basis so gains/losses
              are always calculated correctly — a deposit doesn't count as "profit" and a withdrawal doesn't count as a "loss."
            </dd>
            <dt>Profile</dt>
            <dd>
              Attach a different risk profile to the portfolio. <strong>Warning:</strong> Changing the profile
              ends the current episode and starts a new one. Episode results are preserved in the lifecycle history.
            </dd>
          </dl>

          <h5>"+ Create Portfolio" button</h5>
          <p>Opens a dialog to create a new portfolio. You'll set:</p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Field</th><th>What it means</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Name</strong></td><td>A descriptive name for the portfolio</td><td>Main FX Portfolio</td></tr>
                <tr><td><strong>Currency</strong></td><td>Base currency (USD, EUR, or GBP)</td><td>USD</td></tr>
                <tr><td><strong>Starting Cash</strong></td><td>Initial capital — cannot be changed later</td><td>$100,000</td></tr>
                <tr><td><strong>Risk Profile</strong></td><td>Which profile's rules to apply (position limits, drawdown stops, crystallization)</td><td>MODERATE_RISK</td></tr>
                <tr><td><strong>Notes</strong></td><td>Optional description</td><td>"FX-focused momentum strategy"</td></tr>
              </tbody>
            </table>
          </div>

          <div className="guide-example">
            <div className="guide-example-title">Example: Depositing Cash</div>
            <p>
              Your portfolio "Main FX" has $87,000 in cash and $15,000 in open positions (equity = $102,000).
              You click <strong>Cash → Deposit → $10,000</strong>. After the deposit:
            </p>
            <ul>
              <li>Cash: $87,000 + $10,000 = <strong>$97,000</strong></li>
              <li>Equity: $102,000 + $10,000 = <strong>$112,000</strong></li>
              <li>P&amp;L stays the same — the deposit is a cost basis adjustment, not a profit</li>
              <li>A <strong>DEPOSIT</strong> event is recorded in the lifecycle timeline</li>
            </ul>
          </div>
        </div>

        <div className="guide-page-section">
          <h4>Tab 2: Profiles</h4>
          <p>
            Risk profiles are reusable templates that define how a portfolio should behave. You can create
            as many profiles as you need and attach them to any portfolio. The table shows each profile's
            settings and how many portfolios are currently using it.
          </p>

          <h5>Profile settings explained</h5>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Setting</th><th>What it controls</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Max Positions</strong></td><td>Maximum number of holdings at once</td><td>10</td></tr>
                <tr><td><strong>Max Position %</strong></td><td>Maximum size of any single position as a % of cash</td><td>8%</td></tr>
                <tr><td><strong>Bust Equity %</strong></td><td>If equity drops below this % of starting cash, the portfolio is "bust"</td><td>50%</td></tr>
                <tr><td><strong>Bust Action</strong></td><td>What happens at bust: Allow Exits Only, Liquidate Next Bar, or Liquidate Immediate</td><td>Allow Exits Only</td></tr>
                <tr><td><strong>Drawdown Stop %</strong></td><td>Maximum peak-to-trough decline before entries are blocked</td><td>15%</td></tr>
              </tbody>
            </table>
          </div>

          <h5>Crystallization settings (collapsible section in the profile editor)</h5>
          <p>
            Crystallization is the process of <strong>locking in gains</strong> when a profit target is reached.
            When triggered, the current episode ends, profits are recorded, and a new episode begins.
          </p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Setting</th><th>What it does</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Enabled</strong></td><td>Turn crystallization on or off</td><td>On</td></tr>
                <tr><td><strong>Profit Target %</strong></td><td>The return that triggers crystallization</td><td>10%</td></tr>
                <tr>
                  <td><strong>Mode</strong></td>
                  <td>
                    <strong>Withdraw Profits:</strong> Gains are withdrawn from the portfolio. New episode starts with original capital.
                    <br /><strong>Rebase (compound):</strong> Gains stay in the portfolio. New episode starts with the higher equity as the new cost basis.
                  </td>
                  <td>Withdraw Profits</td>
                </tr>
                <tr><td><strong>Cooldown Days</strong></td><td>Minimum days between crystallization events</td><td>30</td></tr>
                <tr><td><strong>Max Episode Days</strong></td><td>Force a new episode after this many days even without hitting the profit target</td><td>90</td></tr>
                <tr><td><strong>Take Profit On</strong></td><td>Check the target at End of Day or Intraday</td><td>End of Day</td></tr>
              </tbody>
            </table>
          </div>

          <div className="guide-example">
            <div className="guide-example-title">Example: Crystallization in action</div>
            <p>
              Your profile has a <strong>10% profit target</strong> in <strong>Withdraw Profits</strong> mode.
              The portfolio started with $100,000. After 6 weeks, equity reaches $110,500 (+10.5%).
            </p>
            <ol style={{marginLeft: '1.5rem', lineHeight: '1.8'}}>
              <li>The pipeline detects the profit target is hit (+10.5% &gt; 10%)</li>
              <li>$10,500 in profits is withdrawn and recorded as a payout</li>
              <li>The current episode (Episode 1) ends with status "CRYSTALLIZED"</li>
              <li>A new episode (Episode 2) starts with $100,000 as the cost basis</li>
              <li>The lifecycle timeline records both the CRYSTALLIZE and EPISODE_START events</li>
            </ol>
            <p>
              If <strong>Rebase</strong> mode were used instead, the $10,500 would stay in the portfolio
              and Episode 2 would start with $110,500 as the new cost basis. This is compounding —
              subsequent profit targets are measured against the higher base.
            </p>
          </div>
        </div>

        <div className="guide-page-section">
          <h4>Tab 3: Lifecycle Timeline</h4>
          <p>
            A visual history of every meaningful event in a portfolio's life. Select a portfolio
            from the dropdown to view its history.
          </p>

          <h5>Charts (4 panels)</h5>
          <dl className="guide-kv guide-kv--wide">
            <dt>Lifetime Equity</dt>
            <dd>A line chart showing equity over time across all lifecycle events. Each dot marks a recorded event (deposit, withdrawal, crystallization, etc.).</dd>
            <dt>Cumulative Lifetime P&amp;L</dt>
            <dd>An area chart showing the running total of profit/loss over time. Green = above zero (profitable), red area appears if it dips below zero.</dd>
            <dt>Cash Flow Events</dt>
            <dd>A bar chart showing money in (green bars for CREATE and DEPOSIT) and money out (red bars for WITHDRAW and CRYSTALLIZE). Helps you see the pattern of cash flows over time.</dd>
            <dt>Cash vs Equity</dt>
            <dd>Two lines: orange for cash on hand, blue for total equity. The gap between them represents the value tied up in open positions.</dd>
          </dl>

          <h5>Event Timeline (below the charts)</h5>
          <p>
            A vertical timeline listing every lifecycle event in chronological order. Each entry shows:
          </p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Element</th><th>What it shows</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>Event Type</strong></td><td>What happened (color-coded dot)</td><td>DEPOSIT, CRYSTALLIZE, EPISODE_START</td></tr>
                <tr><td><strong>Timestamp</strong></td><td>When it happened</td><td>Feb 7, 2026, 14:30</td></tr>
                <tr><td><strong>Amount</strong></td><td>Money involved (if applicable)</td><td>+$10,000 or -$5,200</td></tr>
                <tr><td><strong>Snapshots</strong></td><td>Cash, Equity, and Lifetime P&amp;L at that moment</td><td>Cash: $97,000 | Equity: $112,000 | P&amp;L: $2,000</td></tr>
                <tr><td><strong>Notes</strong></td><td>Optional notes recorded with the event</td><td>"Quarterly deposit"</td></tr>
              </tbody>
            </table>
          </div>

          <h5>Lifecycle event types</h5>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Event Type</th><th>When it happens</th><th>What it records</th></tr>
              </thead>
              <tbody>
                <tr><td><strong>CREATE</strong></td><td>Portfolio is first created</td><td>Initial cash, starting equity</td></tr>
                <tr><td><strong>DEPOSIT</strong></td><td>You add cash to the portfolio</td><td>Deposit amount, new cash/equity balances</td></tr>
                <tr><td><strong>WITHDRAW</strong></td><td>You remove cash from the portfolio</td><td>Withdrawal amount, new balances</td></tr>
                <tr><td><strong>CRYSTALLIZE</strong></td><td>Profit target hit, gains locked in</td><td>Payout amount, mode (withdraw or rebase)</td></tr>
                <tr><td><strong>PROFILE_CHANGE</strong></td><td>Risk profile is changed</td><td>Old and new profile references</td></tr>
                <tr><td><strong>EPISODE_START</strong></td><td>A new episode begins</td><td>New episode ID, starting equity</td></tr>
                <tr><td><strong>EPISODE_END</strong></td><td>An episode ends</td><td>Final equity, end reason</td></tr>
                <tr><td><strong>BUST</strong></td><td>Portfolio hits bust threshold</td><td>Equity at bust</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="guide-page-section">
          <h4>Tab 4: Portfolio Story</h4>
          <p>
            An AI-generated narrative that tells the complete story of a portfolio — from creation through
            every deposit, withdrawal, crystallization event, and market performance period. Think of it as
            a "biography" for your portfolio.
          </p>

          <dl className="guide-kv guide-kv--wide">
            <dt>Headline</dt>
            <dd>A one-line summary of the portfolio's journey. Example: "Main FX Portfolio: 3 episodes, $5,200 in payouts, currently active with +2.4% return."</dd>
            <dt>Narrative</dt>
            <dd>Multiple paragraphs explaining the full story — how the portfolio started, what happened in each episode, how it responded to market conditions, and where it stands now.</dd>
            <dt>Key Moments</dt>
            <dd>A bulleted list of the most significant events. Example: "Episode 1 crystallized at +12.3% after 45 days" or "Deposit of $10,000 on Feb 7 increased the capital base."</dd>
            <dt>Outlook</dt>
            <dd>Forward-looking commentary based on current state. Example: "With the risk gate in SAFE mode and 4 of 10 position slots available, the portfolio is well-positioned for new opportunities."</dd>
          </dl>

          <div className="guide-callout">
            <strong>Auto-generation:</strong> The story is generated automatically the first time you visit the tab
            for a portfolio. You can manually regenerate it by clicking the <strong>Regenerate</strong> button.
            Generation uses Snowflake Cortex AI and typically takes 10–20 seconds.
          </div>
        </div>
      </section>

      {/* ─── 15. TRAINING STATUS ─── */}
      <section className="guide-section" id="page-training">
        <h2>15. Training Status</h2>
        <p className="guide-page-purpose">
          Monitor the learning process for every symbol/pattern combination. See which
          symbols are gathering evidence, which are close to earning trust, and which are
          already trade-eligible.
        </p>

        <h3>Global Training Digest (top of page)</h3>
        <p>
          An AI-generated narrative covering training progress system-wide. Same format as the Cockpit
          (headline, what changed, what matters, waiting for) but focused entirely on training.
        </p>

        <h3>Filters</h3>
        <p>
          Use the <strong>Market Type</strong> dropdown (FX, STOCK, etc.) and the <strong>Symbol Search</strong>
          input to narrow the table to specific assets.
        </p>

        <h3>Training Table — Every Column Explained</h3>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Column</th><th>What it means</th><th>Where the number comes from</th><th>Example</th></tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>Market Type</strong></td>
                <td>Asset class (FX, STOCK, CRYPTO, etc.)</td>
                <td>From the pattern definition</td>
                <td>FX</td>
              </tr>
              <tr>
                <td><strong>Symbol</strong></td>
                <td>Specific asset being tracked</td>
                <td>From the recommendation log</td>
                <td>AUD/USD</td>
              </tr>
              <tr>
                <td><strong>Pattern</strong></td>
                <td>Which signal pattern is being evaluated</td>
                <td>Pattern ID from pattern definitions</td>
                <td>2</td>
              </tr>
              <tr>
                <td><strong>Interval</strong></td>
                <td>Bar interval in minutes (1440 = daily)</td>
                <td>From pattern definition</td>
                <td>1440</td>
              </tr>
              <tr>
                <td><strong>As Of</strong></td>
                <td>Date this training data was last updated</td>
                <td>Timestamp of last pipeline run</td>
                <td>2026-02-07</td>
              </tr>
              <tr>
                <td><strong>Maturity</strong></td>
                <td>
                  Stage badge + score (0–100) with progress bar.
                  <br />See <a href="#training-stages">Section 5</a> for stage definitions.
                </td>
                <td>Calculated from sample size (30%) + coverage (40%) + horizons (30%)</td>
                <td>CONFIDENT (82)</td>
              </tr>
              <tr>
                <td><strong>Sample Size</strong></td>
                <td>Total number of signals (recommendations) generated</td>
                <td>Count of rows in RECOMMENDATION_LOG for this symbol/pattern</td>
                <td>45</td>
              </tr>
              <tr>
                <td><strong>Coverage</strong></td>
                <td>What % of signals have been fully evaluated across horizons</td>
                <td>Evaluated outcomes ÷ (signals × expected horizons) × 100</td>
                <td>92%</td>
              </tr>
              <tr>
                <td><strong>Horizons</strong></td>
                <td>How many of the 5 evaluation windows have data</td>
                <td>Count of distinct horizon_bars with outcomes</td>
                <td>5</td>
              </tr>
              <tr>
                <td><strong>Avg H1</strong></td>
                <td>Average outcome return at the 1-bar horizon</td>
                <td>Mean of realized_return for all outcomes at horizon_bars = 1</td>
                <td>+0.0032</td>
              </tr>
              <tr>
                <td><strong>Avg H3</strong></td>
                <td>Average outcome return at the 3-bar horizon</td>
                <td>Same calculation for horizon_bars = 3</td>
                <td>+0.0058</td>
              </tr>
              <tr>
                <td><strong>Avg H5</strong></td>
                <td>Average outcome return at the 5-bar horizon</td>
                <td>Same calculation for horizon_bars = 5</td>
                <td>+0.0081</td>
              </tr>
              <tr>
                <td><strong>Avg H10</strong></td>
                <td>Average outcome return at the 10-bar horizon</td>
                <td>Same calculation for horizon_bars = 10</td>
                <td>+0.0045</td>
              </tr>
              <tr>
                <td><strong>Avg H20</strong></td>
                <td>Average outcome return at the 20-bar horizon</td>
                <td>Same calculation for horizon_bars = 20</td>
                <td>-0.0012</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className="guide-example">
          <div className="guide-example-title">Reading Avg H columns</div>
          <p>
            The "Avg H5" column shows +0.0081 for AUD/USD. This means that, on average, 5 trading days after the
            pattern fired, the price had moved +0.81% in the right direction. Positive values are good — they
            indicate the pattern has historically been profitable at that horizon.
          </p>
          <p>
            The "Avg H20" column shows -0.0012 — meaning at the 20-day horizon, the average return is slightly
            negative (-0.12%). This tells you the momentum doesn't persist that long for this particular
            symbol/pattern. The 5-bar horizon is the sweet spot.
          </p>
        </div>

        <h3>Expanded Row (click any row)</h3>
        <p>Clicking a row expands it to show two additional components:</p>
        <dl className="guide-kv guide-kv--wide">
          <dt>Per-Symbol Training Digest</dt>
          <dd>An AI-generated narrative specific to this symbol/pattern. Describes what changed in training, whether it's close to trust, and what outcomes the system is waiting for. Same format as the global digest but focused on one symbol.</dd>
          <dt>Training Timeline Chart</dt>
          <dd>A chart showing the training metrics over time for this symbol. Helps you see if the hit rate and average return are trending up (good) or down (concerning).</dd>
        </dl>
      </section>

      {/* ─── 16. SUGGESTIONS ─── */}
      <section className="guide-section" id="page-suggestions">
        <h2>16. Suggestions</h2>
        <p className="guide-page-purpose">
          Ranked list of trading candidates based on historical performance. These are NOT predictions —
          they're ranked by which symbol/pattern pairs have the best track record.
        </p>

        <h3>Two tiers of candidates</h3>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Tier</th><th>Requirement</th><th>Meaning</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>Strong Candidates</strong></td><td>Sample size ≥ 10, Horizons ≥ 3</td><td>Enough data for reasonable confidence. These are the primary candidates.</td></tr>
              <tr><td><strong>Early Signals</strong></td><td>Sample size ≥ 3, Horizons ≥ 3</td><td>Very early data — treat with caution. Shown with a "Low confidence" badge.</td></tr>
            </tbody>
          </table>
        </div>

        <h3>What each Suggestion Card shows</h3>
        <dl className="guide-kv guide-kv--wide">
          <dt>Rank (#1, #2, ...)</dt>
          <dd>Position in the ranking, ordered by Suggestion Score (highest first).</dd>
          <dt>Symbol / Market / Pattern triple</dt>
          <dd>The specific combination being ranked, e.g., "AAPL / STOCK / pattern 2".</dd>
          <dt>Suggestion Score</dt>
          <dd>
            A transparent, deterministic score combining three factors:
            <p className="guide-formula">
              Score = 0.6 × maturity_score + 0.2 × (mean_return × 1000) + 0.2 × (pct_positive × 100)
            </p>
            <strong>Example:</strong> Maturity 80, mean_return 0.008 (0.8%), pct_positive 0.75 (75%):<br />
            Score = 0.6 × 80 + 0.2 × (0.008 × 1000) + 0.2 × (0.75 × 100) = 48 + 1.6 + 15 = <strong>64.60</strong>
          </dd>
          <dt>Sample Size</dt>
          <dd>Number of signals evaluated. Higher = more evidence. Example: 45.</dd>
          <dt>Maturity Stage + Bar</dt>
          <dd>Stage badge (CONFIDENT, LEARNING, etc.) with a progress bar showing the score (0–100).</dd>
          <dt>What History Suggests</dt>
          <dd>Two plain-language lines summarizing the historical evidence.
            <br /><strong>Line 1:</strong> "Based on 45 recommendations and 40 evaluated outcomes, positive at 75.0% over the strongest horizon (5 bars)."
            <br /><strong>Line 2:</strong> "Mean return at that horizon: 0.81%."
          </dd>
          <dt>Horizon Strip</dt>
          <dd>
            Five mini-bars (sparkline) representing the five horizons (1, 3, 5, 10, 20 bars).
            Below each bar is the percentage — either pct_positive or mean_return.
            <br /><strong>Example:</strong> 1d: 62.5% | 3d: 68.0% | 5d: 75.0% | 10d: 55.0% | 20d: 42.0%
            <br />This tells you the pattern works best at the 5-day horizon (75% positive).
          </dd>
          <dt>Effective Score (early signals only)</dt>
          <dd>
            For early signals, the score is penalized for small sample size:
            <p className="guide-formula">
              Effective Score = Score × min(1, recs_total / 10)
            </p>
            <strong>Example:</strong> Score 50 with only 5 samples: Effective = 50 × (5/10) = <strong>25.0</strong>
          </dd>
        </dl>

        <h3>Evidence Drawer (click a card)</h3>
        <p>Click any card to open a detailed evidence panel with charts and data:</p>
        <dl className="guide-kv guide-kv--wide">
          <dt>Horizon Strip Charts (bar charts)</dt>
          <dd>
            Three bar charts showing performance across all 5 horizons:
            <br /><strong>Average Return:</strong> Mean return at each horizon. Bars above 0% are profitable.
            <br /><strong>% Positive:</strong> What fraction of outcomes were positive. Above 50% means more winners than losers.
            <br /><strong>Hit Rate:</strong> What fraction exceeded the minimum threshold. Similar to % positive but against the threshold.
          </dd>
          <dt>Distribution Histogram</dt>
          <dd>
            A histogram of all realized returns for the selected horizon. Shows the shape of the return distribution.
            The <span style={{color:'#c62828'}}>red dashed line</span> is the mean, and the <span style={{color:'#1565c0'}}>blue dashed line</span> is the median.
            <br />Use the horizon selector buttons (1, 3, 5, 10, 20 days) to switch between horizons.
            <br /><strong>What to look for:</strong> A healthy pattern has most returns on the positive side, with the mean clearly above zero.
          </dd>
          <dt>Confidence Panel</dt>
          <dd>Shows maturity score (progress bar), coverage ratio (%), and reason strings explaining the maturity assessment.</dd>
          <dt>Data Table</dt>
          <dd>Raw numbers for each horizon: N (sample size), Mean realized return, % positive, % hit, Min return, Max return. All returns are in percentage format.</dd>
        </dl>
      </section>

      {/* ─── 17. SIGNALS EXPLORER ─── */}
      <section className="guide-section" id="page-signals">
        <h2>17. Signals Explorer</h2>
        <p className="guide-page-purpose">
          Browse raw signal data — every detection the system has made. Use this page to
          see exactly which signals were generated today, their trust labels, and whether
          they're eligible for trading.
        </p>

        <h3>Filters</h3>
        <p>
          You can filter signals by symbol, market type, pattern, horizon, pipeline run ID, timestamp,
          and trust label. Filters can be set via URL parameters (often linked from other pages)
          or manually adjusted on this page.
        </p>

        <h3>Signals Table — Every Column Explained</h3>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Column</th><th>What it means</th><th>Example</th></tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>Symbol</strong></td>
                <td>Which asset generated this signal</td>
                <td>EUR/USD</td>
              </tr>
              <tr>
                <td><strong>Market</strong></td>
                <td>Asset class</td>
                <td>FX</td>
              </tr>
              <tr>
                <td><strong>Pattern</strong></td>
                <td>Which pattern definition detected this signal</td>
                <td>2</td>
              </tr>
              <tr>
                <td><strong>Score</strong></td>
                <td>
                  The signal strength — typically the observed return at the moment of detection.
                  <br />Higher = stronger detection.
                </td>
                <td>0.0035 (meaning +0.35%)</td>
              </tr>
              <tr>
                <td><strong>Trust</strong></td>
                <td>
                  Current trust label for this symbol/pattern combination.
                  <br /><span style={{color:'#2e7d32'}}>TRUSTED</span> = can generate proposals.
                  <span style={{color:'#f57c00'}}> WATCH</span> = monitoring.
                  <span style={{color:'#c62828'}}> UNTRUSTED</span> = not eligible.
                </td>
                <td><span style={{color:'#2e7d32'}}>TRUSTED</span></td>
              </tr>
              <tr>
                <td><strong>Action</strong></td>
                <td>The recommended action — typically BUY or SELL</td>
                <td>BUY</td>
              </tr>
              <tr>
                <td><strong>Eligible</strong></td>
                <td>
                  Whether this signal can become a trade proposal.
                  <br /><strong>✓</strong> = eligible (trusted pattern, risk gate allows).
                  <br />If not eligible, shows the <strong>gating reason</strong> (e.g., "NOT_TRUSTED", "Z_SCORE_BELOW_THRESHOLD").
                </td>
                <td>✓ or "Z_SCORE_BELOW_THRESHOLD"</td>
              </tr>
              <tr>
                <td><strong>Signal Time</strong></td>
                <td>When the signal was generated</td>
                <td>2026-02-07 14:30</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div className="guide-example">
          <div className="guide-example-title">Reading the Signals Table</div>
          <p>
            You see a row: AUD/USD | FX | Pattern 2 | Score 0.0035 | TRUSTED | BUY | ✓ | 2026-02-07
          </p>
          <p>
            This means: Today, the FX_MOMENTUM_DAILY pattern detected a +0.35% move in AUD/USD.
            The pattern is TRUSTED (it has a proven track record). The signal IS eligible to become
            a trade proposal (✓). If the risk gate allows and the portfolio has capacity, this could
            result in a BUY order for AUD/USD.
          </p>
        </div>

        <h3>Fallback Banner</h3>
        <p>
          If no signals match your filters for the current run, the system automatically tries a broader
          search (fallback). A yellow banner appears explaining what happened and offering actions:
          "Clear all filters," "Use latest run," or "Back to Cockpit."
        </p>
      </section>

      {/* ─── 18. MARKET TIMELINE ─── */}
      <section className="guide-section" id="page-market-timeline">
        <h2>18. Market Timeline</h2>
        <p className="guide-page-purpose">
          End-to-end symbol observability. See every symbol as a tile showing signal, proposal,
          and trade counts. Click a tile to see the price chart with event overlays.
        </p>

        <h3>Overview Grid</h3>
        <p>
          Each tile represents one symbol (e.g., AAPL, EUR/USD) and shows:
        </p>
        <dl className="guide-kv guide-kv--wide">
          <dt>S: (number)</dt>
          <dd><strong>Signals count</strong> — how many signals were detected for this symbol in the selected window (e.g., last 30 bars). Example: S:12 means 12 detections.</dd>
          <dt>P: (number)</dt>
          <dd><strong>Proposals count</strong> — how many of those became trade proposals. Example: P:3.</dd>
          <dt>T: (number)</dt>
          <dd><strong>Trades count</strong> — how many were actually executed. Example: T:1.</dd>
          <dt>ACTION badge</dt>
          <dd>Highlighted in yellow if there are proposals TODAY that may require attention.</dd>
          <dt>Trust badge</dt>
          <dd>TRUSTED / WATCH / UNTRUSTED label for this symbol.</dd>
        </dl>

        <h3>Tile colors</h3>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Color</th><th>Meaning</th></tr>
            </thead>
            <tbody>
              <tr><td><span className="guide-tile-swatch" style={{background:'#e8f5e9'}} /> Green border</td><td><strong>Executed</strong> — trades happened for this symbol</td></tr>
              <tr><td><span className="guide-tile-swatch" style={{background:'#fff3e0'}} /> Orange border</td><td><strong>Proposed</strong> — proposals exist but not yet executed</td></tr>
              <tr><td><span className="guide-tile-swatch" style={{background:'#e3f2fd'}} /> Blue border</td><td><strong>Signals only</strong> — signals detected but no proposals</td></tr>
              <tr><td><span className="guide-tile-swatch" style={{background:'#f5f5f5'}} /> Grey</td><td><strong>Inactive</strong> — no signals in this window</td></tr>
            </tbody>
          </table>
        </div>

        <h3>Filters</h3>
        <p>
          <strong>Portfolio:</strong> Filter to signals/proposals/trades for a specific portfolio or "All."
          <br /><strong>Market:</strong> Filter by market type (FX, STOCK, etc.).
          <br /><strong>Window:</strong> How many bars of history to show (30, 60, 90, or 180 bars).
        </p>

        <h3>Expanded Detail (click a tile)</h3>
        <p>Clicking a symbol tile expands it to show a detailed view:</p>
        <dl className="guide-kv guide-kv--wide">
          <dt>Price Chart (OHLC)</dt>
          <dd>
            A line chart showing the <span style={{color:'#1976d2'}}>close price (blue solid line)</span> with
            <span style={{color:'#bbb'}}> high/low range (grey dashed lines)</span>.
            <br />Event markers are overlaid on the chart:
            <br /><span style={{color:'#2196f3'}}>Blue circles</span> = Signal fired (pattern detected something)
            <br /><span style={{color:'#ff9800'}}>Orange circles</span> = Proposal generated (trade suggested)
            <br /><span style={{color:'#4caf50'}}>Green circles</span> = Trade executed (position opened)
          </dd>
          <dt>Counts Bar</dt>
          <dd>Shows the signal → proposal → trade funnel: "12 signals → 3 proposals → 1 trade"</dd>
          <dt>Decision Narrative</dt>
          <dd>Bullet points explaining what happened and why. Example: "Pattern 2 detected momentum, but risk gate was in CAUTION mode, blocking new entries."</dd>
          <dt>Trust Status by Pattern</dt>
          <dd>Shows trust label and coverage for each pattern tracking this symbol.</dd>
          <dt>Recent Events Table</dt>
          <dd>Last 20 events (signals, proposals, trades) with dates, types, and details like price, quantity, and portfolio links.</dd>
        </dl>
      </section>

      {/* ─── 19. RUNS (AUDIT VIEWER) ─── */}
      <section className="guide-section" id="page-runs">
        <h2>19. Runs (Audit Viewer)</h2>
        <p className="guide-page-purpose">
          Monitor every pipeline run — when it happened, whether it succeeded, how long each step took,
          and what errors occurred. This is your pipeline health dashboard.
        </p>

        <h3>Left Panel: Run List</h3>
        <p>
          A list of recent pipeline runs, showing status, time, and run ID. Click a run to see its details.
        </p>
        <dl className="guide-kv guide-kv--wide">
          <dt>Status Badge</dt>
          <dd>
            <span style={{color:'#2e7d32'}}>Green (SUCCESS)</span> — pipeline completed normally.
            <span style={{color:'#f57c00'}}> Yellow (SUCCESS WITH SKIPS)</span> — completed but some steps were skipped (e.g., no new data).
            <span style={{color:'#c62828'}}> Red (FAILED)</span> — one or more steps had errors.
            <span style={{color:'#1565c0'}}> Blue (RUNNING)</span> — pipeline is currently in progress.
          </dd>
          <dt>Started Time</dt>
          <dd>When the run started, formatted as "Feb 07, 14:30".</dd>
          <dt>Run ID</dt>
          <dd>Unique identifier (first 8 characters shown). Useful for debugging.</dd>
        </dl>

        <h3>Filters</h3>
        <p>
          Filter runs by <strong>Status</strong> (All, Failed, Success, Success with Skips, Running),
          <strong> From date</strong>, and <strong>To date</strong>.
        </p>

        <h3>Right Panel: Run Detail</h3>
        <p>When you click a run, the detail panel shows:</p>
        <dl className="guide-kv guide-kv--wide">
          <dt>Summary Cards</dt>
          <dd>
            <strong>Status:</strong> Final outcome. <strong>Duration:</strong> Total time in seconds.
            <strong> As-of:</strong> Market data date. <strong>Portfolios:</strong> How many portfolios were processed.
            <strong> Errors:</strong> Error count (if any).
          </dd>
          <dt>Run Summary Narrative</dt>
          <dd>An AI-generated or deterministic summary: headline, what happened, why, impact, and next check time.</dd>
          <dt>Error Panel (if errors exist)</dt>
          <dd>Lists each error with event name, timestamp, error message, SQLSTATE code, and query ID. Useful for Snowflake debugging.</dd>
          <dt>Step Timeline</dt>
          <dd>A chronological list of every step the pipeline executed. Each shows: step name, status (✓/✗/○), duration in seconds, and portfolio ID.
            <br /><strong>Example:</strong> "✓ SP_GENERATE_SIGNALS — 2.3s — Portfolio 1" means signal generation took 2.3 seconds and succeeded.</dd>
          <dt>Step Detail Panel</dt>
          <dd>Click a step to see: duration (seconds + ms), rows affected, portfolio ID, timestamps, and error details if it failed.</dd>
        </dl>
      </section>

      {/* ─── 20. DEBUG ─── */}
      <section className="guide-section" id="page-debug">
        <h2>20. Debug</h2>
        <p className="guide-page-purpose">
          A technical health check page. Calls the backend API endpoints one by one and shows
          whether each responds correctly. Primarily for developers and system administrators.
        </p>

        <h3>What it does</h3>
        <p>
          Automatically fires requests to 5 key API endpoints and reports the results:
        </p>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Endpoint</th><th>What it checks</th></tr>
            </thead>
            <tbody>
              <tr><td><code>/api/status</code></td><td>Is the API server running?</td></tr>
              <tr><td><code>/api/runs</code></td><td>Can we fetch pipeline runs?</td></tr>
              <tr><td><code>/api/portfolios</code></td><td>Can we fetch the portfolio list?</td></tr>
              <tr><td><code>/api/digest/latest</code></td><td>Is the latest digest available?</td></tr>
              <tr><td><code>/api/training/status</code></td><td>Is training data accessible?</td></tr>
            </tbody>
          </table>
        </div>

        <h3>How to read results</h3>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Status</th><th>Meaning</th><th>Action</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>200</strong></td><td>Success — endpoint is working</td><td>No action needed</td></tr>
              <tr><td><strong>404</strong></td><td>Not found — endpoint doesn't exist</td><td>Check if the API server is running the correct version</td></tr>
              <tr><td><strong>500</strong></td><td>Server error — backend crashed</td><td>Check Snowflake credentials and database connectivity</td></tr>
              <tr><td><strong>0 (network)</strong></td><td>Cannot reach server at all</td><td>Check if the API server is running, check proxy/CORS settings</td></tr>
            </tbody>
          </table>
        </div>
        <p>
          The <strong>Copy diagnostics</strong> button copies all results as JSON to your clipboard —
          useful for sharing with support.
        </p>
      </section>

      {/* ─── 21. PARALLEL WORLDS ─── */}
      <section className="guide-section" id="page-parallel-worlds">
        <h2>21. Parallel Worlds</h2>
        <p className="guide-page-purpose">
          A "what-if" laboratory for your trading rules. Parallel Worlds replays each day's real market data
          through alternative rule sets and shows you what <em>would</em> have happened — without risking a
          single dollar. It's like having a time machine that lets you test different decisions after the fact.
        </p>

        <div className="guide-example">
          <div className="guide-example-title">Real-World Analogy</div>
          <p>
            Imagine you're a chess player reviewing a game you just finished. You wonder: "What if I'd
            moved the bishop instead of the knight on move 12?" You replay from that point and discover
            you would have won 3 moves sooner. That's Parallel Worlds — except instead of chess moves,
            you're replaying market days with different trading rules and seeing whether those rules
            would have made you more or less money.
          </p>
        </div>

        {/* ── What Is Parallel Worlds? ── */}
        <div className="guide-page-section">
          <h3>What Is Parallel Worlds?</h3>
          <p>
            Every day, MIP runs your portfolio using your current rules: signal thresholds, position sizes,
            entry timing, and risk gates. Parallel Worlds takes that <em>same</em> day's data and asks:
          </p>
          <ul>
            <li><strong>What if we'd used a looser signal filter?</strong> — Would more trades have passed, and would they have been profitable?</li>
            <li><strong>What if we'd used a tighter filter?</strong> — Would being pickier have avoided losses?</li>
            <li><strong>What if we'd used bigger (or smaller) positions?</strong> — Would the extra (or reduced) exposure have helped?</li>
            <li><strong>What if we'd waited a day before entering?</strong> — Would patience have gotten a better price?</li>
            <li><strong>What if we'd done nothing at all?</strong> — Would staying in cash have been the best move?</li>
          </ul>
          <p>
            Each of these alternatives is called a <strong>scenario</strong>. MIP runs all of them every day,
            records the results, and builds up a history so you can see which alternative rules — if any —
            <em>consistently</em> outperform your current approach.
          </p>

          <div className="guide-callout">
            <strong>Key point:</strong> Parallel Worlds never changes your real portfolio. It's a read-only,
            retrospective analysis. Think of it as a flight simulator — you learn from the replay without
            any risk to the real plane.
          </div>
        </div>

        {/* ── Policy Health Card ── */}
        <div className="guide-page-section">
          <h4>Policy Health Card (top of page)</h4>
          <p>
            This card gives you an instant, at-a-glance answer to the question: <strong>"Are my current
            trading rules the best they can be?"</strong> It combines signal confidence, regret, and stability
            into a single health assessment.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Health Badge</dt>
            <dd>
              A colored badge in the top-right corner. Possible values:
              <br /><strong>Healthy</strong> (green) — No alternative rules reliably beat yours. Your approach is solid.
              <br /><strong>Watch</strong> (blue) — Some early signals, but nothing definitive yet. Keep an eye on it.
              <br /><strong>Monitor</strong> (orange) — A couple of scenarios are showing emerging patterns. Worth tracking.
              <br /><strong>Review Suggested</strong> (orange) — One scenario consistently outperforms. Consider studying it.
              <br /><strong>Needs Attention</strong> (red) — Multiple scenarios beat your rules. Time to investigate.
              <br /><strong>Example:</strong> If you see <em>"Healthy"</em>, that means none of the alternative scenarios have
              found a reliable way to beat your current approach — your rules are working well.
            </dd>
            <dt>Stability Gauge (0–100)</dt>
            <dd>
              A progress bar showing how "settled" your rules are. <strong>100</strong> means every alternative
              scenario is noise — your rules are rock-solid. <strong>0</strong> would mean every alternative beats
              you (extremely unlikely in practice).
              <br /><strong>Example:</strong> "Stability 95/100, Very Stable" means 95% of alternative scenarios
              can't beat you. Only 5% show any signal at all — and even those may be weak.
            </dd>
            <dt>Signal Breakdown</dt>
            <dd>
              Small colored badges showing how many scenarios fall into each confidence tier:
              Strong (green), Emerging (blue), Weak (orange), Noise (gray). Most of the time,
              you'll see mostly Noise badges — that's a good thing.
              <br /><strong>Example:</strong> "Noise: 7, Weak: 1" means 7 out of 8 scenarios show no meaningful
              difference, and 1 shows a faint pattern that isn't reliable yet.
            </dd>
            <dt>Biggest Regret Area</dt>
            <dd>
              Which <em>category</em> of rule changes accounts for the most regret (missed opportunity).
              Categories are: Signal Filter, Position Size, Entry Timing, and Baseline (doing nothing).
              <br /><strong>Example:</strong> "Baseline — $378 cumulative regret" means that over all the days measured,
              doing absolutely nothing (staying in cash) would have avoided $378 in losses. That doesn't
              mean "stop trading" — it means there were some rough days where cash was king.
            </dd>
            <dt>Top Candidate</dt>
            <dd>
              If any scenario shows a non-noise signal, the strongest one appears here by name.
              <br /><strong>Example:</strong> "Stay in Cash (No Trades) — Baseline" would appear if the
              do-nothing scenario had the only detectable signal. This is informational, not a recommendation.
            </dd>
          </dl>

          <div className="guide-example">
            <div className="guide-example-title">Example: Reading the Policy Health Card</div>
            <p>
              You open Parallel Worlds and see: <strong>Healthy</strong>, Stability <strong>95/100</strong>,
              Signal Breakdown: <em>Noise: 7, Weak: 1</em>. The italic text at the bottom says:
              "No scenarios reliably beat your current approach."
            </p>
            <p>
              This tells you: your trading rules are performing well relative to alternatives. The one "Weak"
              scenario is not consistent enough to worry about. No action needed — just keep running the pipeline
              and the system will keep checking every day.
            </p>
          </div>
        </div>

        {/* ── AI Narrative Card ── */}
        <div className="guide-page-section">
          <h4>Parallel Worlds Analysis (AI Narrative)</h4>
          <p>
            An AI-written summary that reads all the numbers and tells you what matters in plain English.
            The AI looks at today's scenario results, the decision traces, the regret trend, and writes
            a brief story about what happened and whether anything is worth paying attention to.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>AI / Fallback Badge</dt>
            <dd>
              <strong>"AI"</strong> (purple gradient) means the narrative was generated by Snowflake Cortex AI,
              grounded in today's actual snapshot data. <strong>"Fallback"</strong> (gray) means the AI was
              unavailable and a simpler template-based summary was used instead.
            </dd>
            <dt>Headline</dt>
            <dd>A one-sentence summary of today's most important parallel worlds finding.
              <br /><strong>Example:</strong> "All 8 scenarios matched actual performance — current rules are well-calibrated."</dd>
            <dt>Gate Analysis</dt>
            <dd>Explains what happened at each decision gate (trust, risk, capacity, signal filters).
              <br /><strong>Example:</strong> "Risk gate was open across all scenarios. The capacity gate held at 2/3 positions for all variants."</dd>
            <dt>What-If Insights</dt>
            <dd>Bullet points for the most noteworthy scenario comparisons.
              <br /><strong>Example:</strong> "Lowering the z-score threshold would not have changed any entries — the signal pool was identical."</dd>
            <dt>Regret Trend</dt>
            <dd>Whether regret is growing, shrinking, or flat over time.
              <br /><strong>Example:</strong> "Cumulative regret for the DO_NOTHING scenario has been climbing — 3 of the last 5 days, cash outperformed."</dd>
            <dt>Consideration</dt>
            <dd>A green-boxed suggestion — <em>not</em> a recommendation, but a thought to consider.
              <br /><strong>Example:</strong> "The timing delay scenario has consistently underperformed. Current instant-entry approach appears optimal."</dd>
          </dl>
        </div>

        {/* ── Scenario Comparison Table ── */}
        <div className="guide-page-section">
          <h4>Scenario Comparison Table</h4>
          <p>
            The main data table. Each row is one alternative universe — a different set of rules applied to
            the same day. The table answers: "How much more (or less) money would each scenario have made?"
          </p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Column</th><th>What It Means</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>What If We Had...</strong></td>
                  <td>The scenario's display name. Examples: "Looser Signal Filter (z-score -0.25)", "Bigger Positions (125%)", "Wait 1 Day Before Entering".</td>
                </tr>
                <tr>
                  <td><strong>Category</strong></td>
                  <td>Which type of rule was changed. Color-coded pill: Signal Filter (blue), Position Size (orange), Entry Timing (purple), Baseline (gray).</td>
                </tr>
                <tr>
                  <td><strong>Scenario PnL</strong></td>
                  <td>The profit or loss this scenario would have produced on that day. This is the "counterfactual" — the number from the alternative universe.</td>
                </tr>
                <tr>
                  <td><strong>vs Actual</strong></td>
                  <td>Scenario PnL minus your real PnL. <strong style={{color:'#198754'}}>Green positive</strong> = scenario was better. <strong style={{color:'#dc3545'}}>Red negative</strong> = your real approach was better. $0 = identical.</td>
                </tr>
                <tr>
                  <td><strong>Equity Impact</strong></td>
                  <td>Same idea as "vs Actual" but applied to total portfolio equity. Shows the dollar difference in end-of-day portfolio value.</td>
                </tr>
                <tr>
                  <td><strong># Trades</strong></td>
                  <td>How many trades the scenario would have taken. Compare to your actual trade count to see if the scenario is more or less active.</td>
                </tr>
                <tr>
                  <td><strong>Signal</strong></td>
                  <td>The confidence badge for this scenario (see Signal Confidence section below). Shows whether the scenario's outperformance is reliable or just noise.</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="guide-example">
            <div className="guide-example-title">Example: Reading a Row</div>
            <p>
              You see a row that says: <strong>"Looser Signal Filter (z-score -0.25)"</strong>, Category: <em>Signal Filter</em>,
              Scenario PnL: <strong>$142</strong>, vs Actual: <strong style={{color:'#198754'}}>+$42</strong>,
              Equity Impact: <strong style={{color:'#198754'}}>+$42</strong>, # Trades: <strong>5</strong>, Signal: <em>Noise</em>.
            </p>
            <p className="guide-example-numbers">
              This means: if you had lowered your signal filter's z-score threshold by 0.25, you would have made
              $142 instead of your actual $100 — a $42 improvement. But the "Noise" badge tells you this is
              probably a one-day fluke, not a reliable pattern. Don't change your rules based on a single green day.
            </p>
          </div>

          <h5>Row Highlighting</h5>
          <p>
            Rows with a light green background are scenarios that <strong>outperformed</strong> your actual result
            on that day. This is purely visual — it doesn't mean the scenario is "better" in a reliable sense.
            Look at the Signal badge for the confidence assessment.
          </p>

          <h5>Expandable Detail Row (Decision Trace)</h5>
          <p>
            Click the arrow on any row to expand it and see the <strong>Decision Trace</strong> — a human-readable
            explanation of what happened at each decision gate in that scenario. Instead of raw numbers, you'll
            see plain English sentences.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Gate Cards</dt>
            <dd>
              Each card represents one decision gate (Threshold, Trust, Sizing, Timing, Baseline). The left
              border is color-coded: <strong style={{color:'#198754'}}>green</strong> = passed,
              <strong style={{color:'#dc3545'}}> red</strong> = blocked,
              <strong style={{color:'#fd7e14'}}> orange</strong> = modified or delayed.
            </dd>
            <dt>Status Label</dt>
            <dd>A small badge inside each card: PASSED, BLOCKED, MODIFIED, DELAYED, or INFO.</dd>
            <dt>Explanation Text</dt>
            <dd>
              A sentence written in plain English explaining what happened.
              <br /><strong>Example (Timing):</strong> "Delayed entry by 1 bar. No trades were affected."
              <br /><strong>Example (Threshold):</strong> "No signals changed eligibility."
              <br /><strong>Example (Baseline):</strong> "This scenario skips all trade entries (stay in cash)."
              <br /><strong>Example (Sizing):</strong> "Position size multiplier: 1.25x. Max position moved from 15.0% to 18.8%. No PnL change."
            </dd>
          </dl>
        </div>

        {/* ── Signal Confidence Panel ── */}
        <div className="guide-page-section">
          <h4>Signal Confidence Panel</h4>
          <p>
            This panel answers: <strong>"Can I trust this scenario's results, or is it just random noise?"</strong>
            A single good day doesn't prove anything. The confidence classifier looks at <em>win-rate</em>
            (how often the scenario beats you), <em>cumulative impact</em> (total dollars of difference),
            and <em>consistency</em> (rolling trend) to sort each scenario into one of four tiers.
          </p>
          <div className="guide-metric-table">
            <table>
              <thead>
                <tr><th>Tier</th><th>Color</th><th>What It Means</th><th>Example</th></tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>Strong</strong></td>
                  <td>Green</td>
                  <td>This scenario reliably outperforms your rules — high win rate, many days of data, positive trend. Worth investigating seriously.</td>
                  <td>"Wins 75% over 15 days, avg +$12/day"</td>
                </tr>
                <tr>
                  <td><strong>Emerging</strong></td>
                  <td>Blue</td>
                  <td>A pattern is forming but it's too early to be sure. Keep watching — it might become Strong, or it might fade.</td>
                  <td>"Wins 60% over 8 days — emerging pattern"</td>
                </tr>
                <tr>
                  <td><strong>Weak</strong></td>
                  <td>Orange</td>
                  <td>Some signal exists but it's not consistent. Could be meaningful, could be coincidence.</td>
                  <td>"Wins 45% but not yet consistent"</td>
                </tr>
                <tr>
                  <td><strong>Noise</strong></td>
                  <td>Gray</td>
                  <td>No meaningful difference from your actual approach. Either too few days of data, negligible dollar impact, or low win rate. Ignore it.</td>
                  <td>"Cumulative impact is negligible ($0.50)" or "Too few days of data (2)"</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="guide-example">
            <div className="guide-example-title">Example: Interpreting the Panel</div>
            <p>
              You see 8 scenarios listed. Seven say "Noise" and one says "Weak — Wins 60% but not yet consistent."
              The Weak scenario is "Stay in Cash (No Trades)" with a cumulative delta of -$104.
            </p>
            <p className="guide-example-numbers">
              This means: the do-nothing approach won on 6 out of 10 days (60%), but overall it still
              lost $104 compared to your actual trading. So even though it "won" more days, the days
              you actually traded profitably were bigger wins. The classifier correctly marks it as
              "Weak" — not reliable enough to act on.
            </p>
          </div>

          <div className="guide-callout">
            <strong>Why Noise is good news:</strong> If most scenarios show "Noise," that means the system
            couldn't find any rule changes that would reliably beat your current approach. Your rules are
            well-calibrated. Think of it like a doctor's checkup — "nothing unusual" is exactly what you
            want to hear.
          </div>
        </div>

        {/* ── Equity Curves Chart ── */}
        <div className="guide-page-section">
          <h4>Equity Curves Chart</h4>
          <p>
            A line chart showing how your portfolio equity evolved over time — and how each scenario
            <em> would have</em> evolved if you'd used different rules from the start.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Solid Blue Line</dt>
            <dd>Your actual portfolio equity over time. This is the ground truth — what really happened.</dd>
            <dt>Dashed Colored Lines</dt>
            <dd>Each dashed line represents one scenario. If a dashed line is <strong>above</strong> the solid
              line, that scenario would have produced more wealth. If <strong>below</strong>, your approach
              was better.
              <br /><strong>Example:</strong> If the "Bigger Positions (125%)" dashed line hovers just above
              your solid line for a week, that scenario would have compounded slightly better — but check
              the confidence tier before drawing conclusions.</dd>
            <dt>Hovering</dt>
            <dd>Hover over any point to see the exact equity value for each line on that date.</dd>
            <dt>Convergence</dt>
            <dd>If all lines are bunched tightly together, the scenarios didn't make much difference. If lines
              diverge widely, small rule changes had a big impact — which is worth understanding why.</dd>
          </dl>

          <div className="guide-example">
            <div className="guide-example-title">Example: Reading the Chart</div>
            <p>
              You see your solid blue line at $50,000 and a dashed purple line (Looser Signal Filter) at $50,200.
              That $200 gap means: over the entire period, loosening the filter would have made about $200 more.
              But if the confidence is "Noise," that $200 could easily flip to -$200 next week.
            </p>
          </div>
        </div>

        {/* ── Regret Attribution ── */}
        <div className="guide-page-section">
          <h4>Regret Attribution</h4>
          <p>
            Think of this as a <strong>report card for your rule categories</strong>. Instead of looking at
            individual scenarios, it groups them by type — Signal Filter, Position Size, Entry Timing,
            Baseline — and tells you which <em>category</em> of rules accounts for the most missed opportunity.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Dominant Regret Driver</dt>
            <dd>
              A highlighted bar at the top showing which rule category has the most cumulative regret.
              Regret here means "the total amount the best scenario in that category would have beaten you by,
              summed across all days."
              <br /><strong>Example:</strong> "Baseline — $378 cumulative regret" means over all measured days,
              staying in cash (the best Baseline scenario) would have avoided $378 in losses.
              This doesn't mean you should stop trading — it means some of your trades had rough days.
            </dd>
            <dt>Category Cards</dt>
            <dd>
              One card per rule category, ranked by regret. Each card shows:
              <br /><strong>Avg win rate</strong> — how often that category's scenarios beat you.
              <br /><strong>Cumulative regret</strong> — total missed opportunity in dollars.
              <br /><strong>Best delta</strong> — the single best scenario's cumulative dollar advantage (or disadvantage).
              <br /><strong>Best scenario name</strong> — which specific scenario in that category was strongest, plus its confidence badge.
            </dd>
            <dt>Blue left border</dt>
            <dd>The card with the blue left border is the dominant driver — the #1 regret category.</dd>
          </dl>

          <div className="guide-example">
            <div className="guide-example-title">Example: Reading the Attribution</div>
            <p>
              You see four cards: Baseline (rank #1, $378 regret), Entry Timing (#2, $31 regret),
              Signal Filter (#3, $0 regret), Position Size (#4, $0 regret).
            </p>
            <p className="guide-example-numbers">
              The takeaway: most of the "missed opportunity" comes from the Baseline category — meaning
              there were days when doing nothing would have been better. Signal filters and position sizes
              made no difference at all (zero regret), so those rules are well-tuned. Entry timing showed
              a tiny $31 regret — negligible. Overall, your rules are working well.
            </p>
          </div>
        </div>

        {/* ── Regret Heatmap ── */}
        <div className="guide-page-section">
          <h4>Regret Heatmap</h4>
          <p>
            A color-coded grid that shows how each scenario performed on each day. Each cell is one
            scenario on one day. It's the most detailed view — you can spot trends, streaks, and outliers
            at a glance.
          </p>
          <dl className="guide-kv guide-kv--wide">
            <dt>Rows</dt>
            <dd>Each row is one scenario (by name). All scenarios appear, sorted alphabetically.</dd>
            <dt>Columns</dt>
            <dd>Each column is one date (short format like "Feb 10"). The most recent date is on the right.</dd>
            <dt>Cell Color</dt>
            <dd>
              <strong style={{color:'#198754'}}>Green</strong> = the scenario beat your actual result that day (positive delta).
              <strong style={{color:'#dc3545'}}> Red</strong> = your actual result was better (negative delta).
              <strong>Gray</strong> = no difference ($0).
              <br />The <em>intensity</em> of the color shows how large the difference was. A bright green
              cell means a big positive delta; a pale green cell means a small one.
            </dd>
            <dt>Cell Value</dt>
            <dd>The dollar amount shown inside each cell. Hover for the full detail tooltip.</dd>
          </dl>

          <div className="guide-example">
            <div className="guide-example-title">Example: Reading the Heatmap</div>
            <p>
              You look at the "Stay in Cash" row and see: Feb 3: <strong style={{color:'#dc3545'}}>-$45</strong>,
              Feb 4: <strong style={{color:'#198754'}}>+$20</strong>,
              Feb 5: <strong style={{color:'#198754'}}>+$60</strong>,
              Feb 6: <strong style={{color:'#dc3545'}}>-$12</strong>,
              Feb 7: <strong style={{color:'#198754'}}>+$15</strong>.
            </p>
            <p className="guide-example-numbers">
              This means: on Feb 3, your actual trading made $45 more than doing nothing — good day.
              On Feb 5, doing nothing would have saved you $60 — bad day for your trades.
              The mix of green and red tells you the cash scenario is not consistently better; it won
              3 of 5 days but the win sizes varied. If the entire row were solid green, that would be
              a much stronger signal that something needs to change.
            </p>
          </div>
        </div>

        {/* ── What Parallel Worlds Is NOT ── */}
        <div className="guide-page-section">
          <h3>What Parallel Worlds Is NOT</h3>
          <div className="guide-callout--warn guide-callout">
            <strong>Important clarifications</strong>
            <br />
            <strong>Not a crystal ball.</strong> Parallel Worlds looks backward, not forward. It tells you what
            <em> would have</em> happened, not what <em>will</em> happen. Past patterns may not repeat.
            <br /><br />
            <strong>Not a recommendation to change your rules.</strong> Even a "Strong" confidence signal
            is informational, not prescriptive. Markets change, and a scenario that outperformed last
            week might underperform next week. Use the data to <em>understand</em> your rules, not to
            blindly change them.
            <br /><br />
            <strong>Not live trading.</strong> No money is at risk. Parallel Worlds never places trades,
            modifies positions, or changes any portfolio settings. It's purely analytical.
            <br /><br />
            <strong>Not random.</strong> Every simulation is <strong>deterministic</strong> — run it twice with
            the same data and you get the same result. There's no randomness, no Monte Carlo sampling.
            The numbers are exact "what-if" calculations.
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════════ */}
      {/* GLOSSARY / QUICK REFERENCE                                     */}
      {/* ═══════════════════════════════════════════════════════════════ */}
      <div className="guide-part-header">Quick Reference</div>

      <section className="guide-section" id="glossary">
        <h2>Key Terms Glossary</h2>
        <div className="guide-metric-table">
          <table>
            <thead>
              <tr><th>Term</th><th>Definition</th></tr>
            </thead>
            <tbody>
              <tr><td><strong>Signal</strong></td><td>A detection by a pattern that interesting price action occurred. Not a trade — just a record.</td></tr>
              <tr><td><strong>Pattern</strong></td><td>A named strategy with specific parameters (min_return, min_zscore, etc.) that scans market data for signals.</td></tr>
              <tr><td><strong>Horizon</strong></td><td>A time window (1, 3, 5, 10, or 20 bars) at which the system evaluates what happened after a signal.</td></tr>
              <tr><td><strong>Hit Rate</strong></td><td>Percentage of evaluated outcomes that were favorable. Threshold: ≥ 55%.</td></tr>
              <tr><td><strong>Avg Return</strong></td><td>Average realized return across all outcomes. Threshold: ≥ 0.05% (0.0005).</td></tr>
              <tr><td><strong>Maturity Score</strong></td><td>A 0–100 score measuring data quality: 30% sample size + 40% coverage + 30% horizons.</td></tr>
              <tr><td><strong>Trust Label</strong></td><td>TRUSTED / WATCH / UNTRUSTED. Determined by passing 3 gates: sample ≥ 40, hit rate ≥ 55%, avg return ≥ 0.05%.</td></tr>
              <tr><td><strong>Proposal</strong></td><td>A suggested trade order generated when a TRUSTED signal passes risk/capacity checks.</td></tr>
              <tr><td><strong>Risk Gate</strong></td><td>Safety mechanism that blocks new entries when portfolio drawdown exceeds a threshold.</td></tr>
              <tr><td><strong>Episode</strong></td><td>A "generation" of the portfolio. Starts at creation or after crystallization/profile change. All KPIs and performance numbers are scoped to the active episode.</td></tr>
              <tr><td><strong>Crystallization</strong></td><td>The process of locking in gains when a profit target is hit. Ends the current episode and starts a new one. Two modes: Withdraw Profits (pay out gains) or Rebase (compound gains into new cost basis).</td></tr>
              <tr><td><strong>Lifecycle Event</strong></td><td>An immutable record of a portfolio state change — CREATE, DEPOSIT, WITHDRAW, CRYSTALLIZE, PROFILE_CHANGE, EPISODE_START, EPISODE_END, or BUST. Stored permanently for audit and timeline views.</td></tr>
              <tr><td><strong>Risk Profile</strong></td><td>A reusable template defining portfolio behavior: position limits, drawdown stops, bust threshold, and crystallization rules. Attached to portfolios and can be changed at any time (which starts a new episode).</td></tr>
              <tr><td><strong>Pipeline Lock</strong></td><td>A safety mechanism that disables all portfolio editing while the daily pipeline is running. Prevents data conflicts. Buttons re-enable automatically once the pipeline completes.</td></tr>
              <tr><td><strong>Deposit / Withdraw</strong></td><td>Cash events that add or remove money from a portfolio without affecting P&amp;L tracking. The system adjusts the cost basis so deposits aren't counted as profit and withdrawals aren't counted as losses.</td></tr>
              <tr><td><strong>Drawdown</strong></td><td>The percentage decline from a portfolio's peak equity. -5% drawdown = 5% below the high water mark.</td></tr>
              <tr><td><strong>Cortex AI</strong></td><td>Snowflake's built-in LLM service used to generate narrative digests and portfolio stories from snapshot data.</td></tr>
              <tr><td><strong>Pipeline</strong></td><td>The daily automated process: fetch data → detect signals → evaluate outcomes → update trust → trade → check crystallization → generate digest.</td></tr>
              <tr><td><strong>Z-Score</strong></td><td>How many standard deviations a value is from the mean. Z-score of 2 means the move is unusually large (2σ above average).</td></tr>
              <tr><td><strong>Coverage Ratio</strong></td><td>What fraction of signals have been fully evaluated across all horizons. 100% = complete evaluation.</td></tr>
              <tr><td><strong>Notional</strong></td><td>The total monetary value of a trade: Price × Quantity. A buy of 100 shares at $150 = $15,000 notional.</td></tr>
              <tr><td><strong>Cost Basis</strong></td><td>The average price at which a position was entered, adjusted for deposits and withdrawals. Used to calculate unrealized profit/loss.</td></tr>
              <tr><td><strong>Portfolio Story</strong></td><td>An AI-generated narrative biography of a portfolio — covering its creation, cash events, episodes, crystallizations, and current outlook. Found in Portfolio Management → Portfolio Story tab.</td></tr>
              <tr><td><strong>Parallel Worlds</strong></td><td>A read-only "what-if" analysis system. Replays each day's market data through alternative rule sets (scenarios) and compares their outcomes to your actual results. Never affects your real portfolio.</td></tr>
              <tr><td><strong>Scenario</strong></td><td>An alternative set of trading rules used in Parallel Worlds. Examples: "Looser Signal Filter," "Bigger Positions (125%)," "Wait 1 Day Before Entering," "Stay in Cash." Each scenario produces a counterfactual PnL.</td></tr>
              <tr><td><strong>Counterfactual</strong></td><td>The hypothetical outcome that would have occurred under different rules. "Counterfactual PnL of $142" means the scenario would have made $142 that day, compared to your actual result.</td></tr>
              <tr><td><strong>Regret</strong></td><td>The dollar amount by which a scenario outperformed your actual result, summed over time. Regret of $50 means the scenario's cumulative advantage is $50. Only positive differences count — days you beat the scenario don't reduce regret.</td></tr>
              <tr><td><strong>Confidence Class</strong></td><td>A reliability tier assigned to each scenario: Strong (reliable outperformance), Emerging (pattern forming), Weak (inconsistent), or Noise (no meaningful signal). Based on win-rate, cumulative impact, and rolling consistency.</td></tr>
              <tr><td><strong>Decision Trace</strong></td><td>A human-readable record of what happened at each decision gate (Trust, Risk, Threshold, Sizing, Timing) during a scenario's simulation. Shows which gates passed, blocked, or modified trades, and explains why in plain English.</td></tr>
              <tr><td><strong>Policy Health</strong></td><td>An at-a-glance assessment of whether your current trading rules are optimal. Combines confidence signals, regret attribution, and stability into a single health rating: Healthy, Watch, Monitor, Review Suggested, or Needs Attention.</td></tr>
              <tr><td><strong>Stability Score</strong></td><td>A 0–100 score measuring how "settled" your trading rules are. 100 = every alternative is noise (very stable). Lower scores mean more scenarios are showing signal, suggesting your rules might benefit from review.</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
