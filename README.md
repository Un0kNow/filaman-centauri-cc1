# Elegoo Centauri Carbon 1 for FilaMan

A native [FilaMan](https://github.com/Fire-Devils/filaman-system) driver for the Elegoo Centauri Carbon 1. The driver uses SDCP over the local network and keeps print usage in FilaMan.

- View live print status, the spool holder and its remaining filament, and reported CANVAS slots.
- Deduct measured usage for completed or canceled single-spool prints. A print canceled at 0 mm records no deduction.
- Browse print previews, recorded deductions, and review queues. When multiple CC1 printers are configured, filter the history by printer.
- Review prints without enough data to book a deduction. Current CANVAS telemetry reports total extrusion, so CANVAS prints that used filament go to **Needs review**.

## Setup

Requires FilaMan 2.3.1 or later. The release was tested on FilaMan 2.3.1 against simulated SDCP traffic and an Elegoo CC1.

1. Use a Centauri Carbon 1 with [OpenCentauri patched firmware](https://docs.opencentauri.cc/patched-firmware/). FilaMan needs network access to the printer on TCP port 3030 and HTTP access for plate previews.
2. Download the installable ZIP from [Releases](https://github.com/Un0kNow/filaman-centauri-cc1/releases) and upload it in FilaMan under **Admin → Plugins → Upload ZIP**.
3. Select **Elegoo Centauri Carbon 1** when adding the printer in FilaMan. Enter its LAN IPv4 address and, if needed for initial discovery, its SDCP Mainboard ID.
4. Assign a FilaMan spool to the printer's holder. Set the filament's diameter and density so measured extrusion can be converted to grams. Disable any previous automatic deduction for the same printer before using this driver.

Open **Elegoo Centauri Carbon 1** from FilaMan's plugin sidebar to see the spool overview and **Prints**.

After upgrading to 0.1.15, existing plate previews remain available in **Prints**. The upgrade moves them out of spool event metadata.

## Build

```sh
python3 -m zipfile -c centauri_cc1.zip __init__.py plugin.json driver.py sdcp.py usage.py page.html icon.svg LICENSE
```

## License

[MIT](LICENSE).
