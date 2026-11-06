# Browser verification

Checked in Chrome with Playwright on November 5, 2026 against the generated standalone HTML, served locally over HTTP.

| Interaction | Observed result |
|---|---|
| Initial November 2011, all countries | Net recorded sales GBP 1,461,756.25; 2,884 orders; 3.2% return-value rate |
| Select December 2011 | Partial-month notice states that data ends on 9 December 2011; net sales updates to GBP 433,668.01 |
| Select France within December | Net sales GBP 7,048.83; 18 orders; 339 source lines; one country row; partial-period notice remains |
| Console after final reload and filter changes | Zero errors and zero warnings |

Values were checked against the generated monthly/country CSV. This verifies the reporting interactions, not production deployment or actual stakeholder adoption.
