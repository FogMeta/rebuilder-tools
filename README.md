# rebuilder-tools

## Build
```sh
git clone https://github.com/FogMeta/rebuilder-tools.git

cd rebuilder-tools

# get submodules
git submodule update --init --recursive

# build filecoin-ffi
make ffi

make
```

## Usage

### init config file

```bash
./rebuildctl init
```

`init` will generate the `rebuilder.conf` file in current directory

config file content is just like the this, just set yours parameters

```toml
[aria2] # for download
  host = ""
  port = 0
  secret = ""

[task]
  input_path = ""  # download path
  output_path = "" # source file path
  parallel = 0     # number of task parallel, default 3

[mcs] # for upload
  api_key = ""
  api_token = ""
  network = ""
  bucket_name = ""

[lotus] # for retrieve
  node_api = ""
  wallet = ""
  timeout = 0
```

### build

`build` try download car files, then rebuild source file, if `build` successfully, will return the `file download url`

1. build with car url

```bash
./rebuildctl build [car_download_urls...]
```

2. build with car metadata json/csv file

```bash
./rebuildctl build --file [metadata.json/metadata.csv]
```

`build` will try rebuild after download car first, if failed, will try `retrieve`

### retrieve

`retrieve` try retrieve file from miner, then rebuild source file, if `retrieve` successfully, will return the `file download url`

1. retrieve with specific cids & miners

```bash
./rebuildctl retrieve --cids='cid1,cid2' --miners='f1,f2'
```
**Note: cids and miners must be matched one by one**

2. retrieve with car metadata json/csv file

```bash
./rebuildctl retrieve --file [metadata.json/metadata.csv]
```

## Contribute

PRs are welcome!




