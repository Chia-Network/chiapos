#!/usr/bin/env bash
set -eo pipefail
_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$_dir/../.."

##
# Usage: fetch_bladebit_harvester.sh <linux|macos|windows> <arm64|x86-64>
#
# Use gitbash or similar under Windows.
##
host_os=$1
host_arch=$2

if [[ "${host_os}" != "linux" ]] && [[ "${host_os}" != "macos" ]] && [[ "${host_os}" != "windows" ]]; then
  echo >&2 "Unkonwn OS '${host_os}'"
  exit 1
fi

if [[ "${host_arch}" != "arm64" ]] && [[ "${host_arch}" != "x86-64" ]]; then
  echo >&2 "Unkonwn Architecture '${host_arch}'"
  exit 1
fi

## Change this if including a new bladebit release
artifact_ver="v3.1.0-rc2"
artifact_base_url="https://github.com/Chia-Network/bladebit/releases/download/v3.1.0-rc2"

linux_arm_sha256="9377126d1bf3e820dd3a4de3a28b3ed1309854b1e808c450ec0414b9b1193990"
linux_x86_sha256="e3fce120863ef8a01e664723d7675770058db0be1d1a389965cc856d2c18277f"
macos_arm_sha256="59eefcbe88c29c83363fb1f16167c921b512aa3ddac8f7eff32d28da3ff3cba1"
macos_x86_sha256="8b41cbbbb30c9bb3e5e718fd578291a244eb3582d82a496e0c8049896c2599d8"
windows_sha256="e591e54993b35a371bd46c93c51c08e5d290494e30dd724e65fffd31d98d32ed"
## End changes

artifact_ext="tar.gz"
sha_bin="sha256sum"
expected_sha256=

if [[ "$OSTYPE" == "darwin"* ]]; then
  sha_bin="shasum -a 256"
fi

case "${host_os}" in
linux)
  if [[ "${host_arch}" == "arm64" ]]; then
    expected_sha256=$linux_arm_sha256
  else
    expected_sha256=$linux_x86_sha256
  fi
  ;;
macos)
  if [[ "${host_arch}" == "arm64" ]]; then
    expected_sha256=$macos_arm_sha256
  else
    expected_sha256=$macos_x86_sha256
  fi
  ;;
windows)
  expected_sha256=$windows_sha256
  artifact_ext="zip"
  ;;
*)
  echo >&2 "Unexpected OS '${host_os}'"
  exit 1
  ;;
esac

# Download artifact
artifact_name="green_reaper.${artifact_ext}"
curl -L "${artifact_base_url}/green_reaper-${artifact_ver}-${host_os}-${host_arch}.${artifact_ext}" >"${artifact_name}"

# Validate sha256, if one was given
if [ -n "${expected_sha256}" ]; then
  gr_sha256="$(${sha_bin} ${artifact_name} | cut -d' ' -f1)"

  if [[ "${gr_sha256}" != "${expected_sha256}" ]]; then
    echo >&2 "GreenReaper SHA256 mismatch!"
    echo >&2 " Got     : '${gr_sha256}'"
    echo >&2 " Expected: '${expected_sha256}'"
    exit 1
  fi
fi

# Unpack artifact
dst_dir="libs/green_reaper"
mkdir -p "${dst_dir}"
if [[ "${artifact_ext}" == "zip" ]]; then
  unzip -d "${dst_dir}" "${artifact_name}"
else
  pushd "${dst_dir}"
  tar -xzvf "../../${artifact_name}"
  popd
fi
