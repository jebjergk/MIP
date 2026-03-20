import InfoTooltip from './InfoTooltip'

export default function GlossaryHoverCard({ scope, entryKey, variant = 'long' }) {
  return <InfoTooltip scope={scope} entryKey={entryKey} variant={variant} />
}
