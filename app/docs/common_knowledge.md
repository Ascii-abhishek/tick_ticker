# Options Common Knowledge

## Call and Put Options

An option gives the buyer a right, not an obligation, to trade an underlying asset at a fixed strike price on or before expiry.

- `CE` means Call European. For NIFTY index options, call value generally rises when the index rises.
- `PE` means Put European. Put value generally rises when the index falls.

## Expiry

Expiry is the date on which the option contract stops trading and settles. NIFTY has weekly and monthly expiries. The platform stores expiry in `expiry_date` so contracts are easy to group for backtests, dashboards, and option-chain analysis.

## Strike Price

Strike price is the fixed price level attached to the option contract, such as `22500 CE` or `22500 PE`. For index options, strikes are usually spaced at regular intervals such as 50 points.

## Open Interest

Open Interest, stored as `open_interest`, is the number of outstanding option contracts that remain open. Rising OI can indicate that new positions are being built; falling OI can indicate positions are being closed. OI is most useful when interpreted with price and volume.

## ATM

ATM means at-the-money. The ATM strike is the strike closest to the current underlying price. If NIFTY is trading at `22482`, the ATM strike with a 50-point step is usually `22500`.

## PCR

PCR means put-call ratio. A common OI-based PCR is:

```text
total_put_open_interest / total_call_open_interest
```

It is stored in `options_snapshot.pcr` for BI-friendly queries.

