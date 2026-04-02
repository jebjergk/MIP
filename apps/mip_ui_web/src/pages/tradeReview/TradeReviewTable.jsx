import {
  formatCommitteeNormalized,
  formatDateTime,
  formatMoney,
  formatReturnFraction,
  labelPwRegretDriver,
} from './formatters.js'

function rowKey(r) {
  return r.closeout_id ?? r.CLOSEOUT_ID ?? ''
}

export default function TradeReviewTable({ trades, selectedRow, onSelectRow }) {
  return (
    <div className="tr-table-wrap">
      <table className="tr-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Entry</th>
            <th>Exit</th>
            <th className="tr-num">Return</th>
            <th className="tr-num">PnL</th>
            <th>Alignment</th>
            <th>Committee</th>
            <th>Override</th>
            <th className="tr-num">PW regret</th>
            <th>PW driver</th>
            <th>Recon</th>
            <th>EIS</th>
          </tr>
        </thead>
        <tbody>
          {(trades || []).map((row) => {
            const id = rowKey(row)
            const sel = selectedRow && rowKey(selectedRow) === id
            const eis = row.has_eis === true || row.has_eis === 1
            const driverLabel = labelPwRegretDriver(row.pw_regret_driver)
            return (
              <tr
                key={id}
                className={`tr-table__row--clickable${sel ? ' tr-table__row--selected' : ''}`}
                onClick={() => onSelectRow(row)}
              >
                <td>{row.symbol ?? row.SYMBOL ?? '—'}</td>
                <td>{formatDateTime(row.entry_ts ?? row.ENTRY_TS)}</td>
                <td>{formatDateTime(row.exit_ts ?? row.EXIT_TS)}</td>
                <td className="tr-num">{formatReturnFraction(row.realized_return ?? row.REALIZED_RETURN)}</td>
                <td className="tr-num">{formatMoney(row.realized_pnl ?? row.REALIZED_PNL)}</td>
                <td><span className="tr-badge-soft">{row.alignment_class ?? '—'}</span></td>
                <td><span className="tr-badge-soft">{formatCommitteeNormalized(row.committee_action_normalized)}</span></td>
                <td><span className="tr-badge-soft">{row.override_class ?? '—'}</span></td>
                <td className="tr-num">{formatMoney(row.pw_regret_amount)}</td>
                <td>{driverLabel ?? '—'}</td>
                <td><span className="tr-badge-soft">{row.reconciliation_class ?? '—'}</span></td>
                <td>{eis ? 'Yes' : 'No'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
