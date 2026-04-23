# Proxy Guide

> Everything you need to know about using proxies with the Google Maps Business Scraper.

---

## What is a proxy and why use one?

A proxy is a middleman server. Instead of your computer talking directly to Google Maps, requests go through an intermediate IP address:

```
Your PC  →  Proxy Server (different IP)  →  Google Maps
```

From Google's perspective, requests come from the proxy's IP, not yours. This means:

- Google cannot identify you as the same user across sessions
- If one IP gets rate-limited, you switch to another automatically
- You appear as many different users, not one persistent scraper

---

## Proxy URL formats

What to put in `config.yaml`:

```yaml
stealth:
  proxies:
    - "http://185.199.229.156:7492"                         # anonymous (no login)
    - "http://user:password@gate.smartproxy.com:7000"       # authenticated (paid)
    - "socks5://192.168.1.1:1080"                           # SOCKS5 protocol
```

---

## Free proxies

Free proxies are unreliable (slow, frequently dead, often blocked by Google) but useful for testing. Expect only 10–30% to work on any given day.

| Source | Notes |
|---|---|
| [free-proxy-list.net](https://free-proxy-list.net/) | Filter by HTTPS=yes. Format: `http://IP:PORT` |
| [sslproxies.org](https://www.sslproxies.org/) | Same format. Filter for HTTPS. |
| [TheSpeedX/PROXY-List](https://github.com/TheSpeedX/PROXY-List) | Updated daily. Use `http.txt`. |
| [clarketm/proxy-list](https://github.com/clarketm/proxy-list) | Another maintained public list. |

### Test a proxy before adding it

```python
import requests

proxy = "http://185.199.229.156:7492"   # ← your proxy here
try:
    r = requests.get(
        "https://httpbin.org/ip",
        proxies={"http": proxy, "https": proxy},
        timeout=5,
    )
    print("WORKS — your IP shown as:", r.json()["origin"])
except Exception as e:
    print("DEAD:", e)
```

Only add proxies that pass this test to `config.yaml`.

---

## Paid proxies (much more reliable)

| Provider | Type | Cost | Format |
|---|---|---|---|
| [BrightData](https://brightdata.com) | Residential | ~$8.4/GB | `http://user:pass@brd.superproxy.io:22225` |
| [Smartproxy](https://smartproxy.com) | Residential | ~$7/GB | `http://user:pass@gate.smartproxy.com:7000` |
| [Oxylabs](https://oxylabs.io) | Residential | Enterprise | `http://user:pass@pr.oxylabs.io:7777` |
| [WebShare](https://webshare.io) | Datacenter | Free tier + paid | `http://user:pass@proxy.webshare.io:80` |
| [IPRoyal](https://iproyal.com) | Datacenter | ~$1.75/GB | `http://user:pass@geo.iproyal.com:12321` |

Residential proxies (BrightData, Smartproxy) look like real home IPs and are far less likely to trigger captchas than datacenter proxies.

---

## How the scraper uses proxies

1. Starts with the first proxy in the list
2. Each new browser session advances to the next proxy (round-robin)
3. If a proxy fails 3 times consecutively → it is automatically removed
4. If **all** proxies fail → falls back to direct connection
5. HTTP enrichment (email/phone fetching) also uses the active proxy

> Proxy credentials are **never** exposed in logs — they are masked as `*****` in all log output.

---

## Recommended strategy

| Use case | Recommendation |
|---|---|
| A few hundred results, one session | No proxies needed. Stealth + human captcha solve is enough. |
| Large mega-mode run (1,000+ results) | Add 3–5 free proxies, or one month of WebShare paid (~$3). |
| Daily automated runs | Use BrightData or Smartproxy residential. Rotate every query with `browser_restart_every: 1`. |

---

## SOCKS5 proxies

SOCKS5 is a lower-level protocol that handles more traffic types than HTTP. Playwright supports it natively. Use it when your provider offers it:

```yaml
stealth:
  proxies:
    - "socks5://username:password@proxy.provider.com:1080"
```

SOCKS5 is generally faster than HTTP proxies for browser traffic.

---

## Troubleshooting

**"Proxy connection refused"**
The proxy is dead. Remove it from `config.yaml` and try another.

**"Still getting captchas with proxies"**
Free proxies are often already flagged by Google. Switch to residential proxies — they look like real home IPs.

**"I only have one proxy"**
That is fine. The scraper will use it for every session. Add more if that one gets blocked.

**"I do not want to use proxies"**
Leave `stealth.proxies` as `[]` in `config.yaml`. The scraper works perfectly without them for normal-sized runs.
