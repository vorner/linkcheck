# Tool to watch (possibly multiple) PrusaLink (and maybe Octoprint) printers

It wants a config file (default at `~/.printers.yaml`).

```yaml
printers:
  mini:
    addr:
      # If the printer address can change...
      - 10.67.22.65:80
      - 10.67.22.122:80
    key: ApiKeyFromMenu
```
