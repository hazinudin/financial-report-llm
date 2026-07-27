# Red-flag / contradiction prompt (Stage 5)

Read the provided fact sheet and computed ratios. Identify up to 10 concrete
contradictions, divergences, or anomalies an analyst MUST address in the
write-up. Examples of the kind of tension to surface:

- profit up but operating cash flow down (collections < revenue)
- receivables / inventory ballooning while revenue is flat or falling
- a line item collapsing to zero vs a material prior-period balance
- margin expansion with cost growth that should exceed revenue growth (sign of a
  reclassification or one-off)
- leverage improving only because of a one-off restricted-cash / asset release
- customer or segment concentration shifting abruptly period-over-period
- net-cash positioning that ignores a large reclamation / closure provision
- goodwill / intangibles steady while the underlying business deteriorates
- dividends paid while debt rises, or vice versa

Output a concise markdown bullet list, one item per line, each naming the two
facts that are in tension and which section of the analysis should address it
(e.g. "§4.2 Receivables +216% while §5.1 collections are 59.6% of revenue").
Do not output anything other than the bullet list.