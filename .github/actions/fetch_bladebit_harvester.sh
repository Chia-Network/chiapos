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

## Change this before releasing 2.0.0
artifact_ver="v3.0.0-rc1"
artifact_base_url="https://github.com/Chia-Network/bladebit/releases/download/v3.0.0-rc1"

linux_arm_sha256="d0af989049f077be726cf5cbc9b5e138defe7891214c23457bd6925fded68b3d"
linux_x86_sha256="d7cb5525e11b27d386523730fcde4dfc099681ccddbd5861cf9c7ba7fbda9676"
macos_arm_sha256="b63b8611791a02395ad3fa31f78f2983335301e8cf3352729026fb0aec9b8e3c"
macos_x86_sha256="8b4ba2cb40a041ba64025f933eecb59942c5d85447d0fef7ded8e8e4527a0573"
windows_sha256="bcfef83e1d9664308d8571b76af57f5a7e4b0fdc14a380deeb21b186b02b2356"
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
