# Validator Setup Guide

Steps to set-up the auditor.

## Setup Steps

0. Clone the repo
```bash
git clone https://github.com/rayonlabs/nineteen.git
cd nineteen
```

1. Install system dependencies (Ubuntu 24.04 LTS):

```bash
export NO_LAUNCH=1
sudo -E ./bootstrap.sh
```

2. Get your key onto the VM:

You know how to do this, don't you ;)

3. Create and set up the `.auditor.env` file:

```bash
task auditor-config
```

4. Install the dependencies:

```bash
task install
```



5. Run the Auditor

```bash
task auditor-autoupdates
```

or

```bash
task auditor
```