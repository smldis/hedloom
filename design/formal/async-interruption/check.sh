#!/usr/bin/env bash
# All downloads, state databases and full logs stay outside the checkout.
set -eu
model_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
java_bin=${JAVA_BIN:-java}
tla_jar=${TLA_JAR:-/tmp/hedloom-tla2tools-1.7.4.jar}
check_dir=${CHECK_DIR:-$(mktemp -d /tmp/hedloom-async-tlc.XXXXXX)}
mkdir -p "$check_dir"
if [ ! -f "$tla_jar" ]; then
  curl --fail --location --silent --show-error \
    https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar \
    --output "$tla_jar"
fi
expected_sha=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
actual_sha=$(sha256sum "$tla_jar")
[ "${actual_sha%% *}" = "$expected_sha" ] || { echo 'Unexpected tools checksum' >&2; exit 1; }
"$java_bin" -version
printf 'Logs and TLC state: %s\n' "$check_dir"
printf 'configuration\tresult\tgenerated\tdistinct\n' > "$check_dir/results.tsv"
cd "$model_dir"
if [ "$#" -eq 0 ]; then
  set -- loss-legacy loss-fixed loss-never-assigned loss-witness-never-assigned loss-witness-certified loss-witness-extra-loss loss-witness-task-identity loss-witness-timeout cleanup-overlap cleanup-worker cleanup-legacy cleanup-fixed cleanup-matching cleanup-owner-only
fi
for configuration in "$@"; do
  module=PoolInterrupt
  expected=pass
  case "$configuration" in
    legacy-memory) expected=NoCollateral ;;
    legacy-rpc) expected=NoStaleRPCFailure ;;
    partition) expected=NoFalseTermination ;;
    unexpected-replay) expected=NoReplay ;;
    error-cleanup-start) expected=NoPostWithdrawalStart ;;
    cleanup-worker) module=CleanupOverlap683a88f ;;
    cleanup-overlap) module=CleanupOverlap683a88f; expected=NoSpuriousAckError ;;
    cleanup-fixed|cleanup-matching) module=CleanupOverlap ;;
    cleanup-legacy|cleanup-owner-only) module=CleanupOverlap; expected=NoSpuriousAckError ;;
    loss-fixed|loss-never-assigned) module=LossEvidence ;;
    loss-legacy) module=LossEvidence; expected=NoFalseClaim ;;
    loss-witness-never-assigned) module=LossEvidence; expected=NoNeverAssignedCancellation ;;
    loss-witness-certified) module=LossEvidence; expected=NoCertifiedCancellation ;;
    loss-witness-extra-loss) module=LossEvidence; expected=NoExtraLossSurvival ;;
    loss-witness-task-identity) module=LossEvidence; expected=NoStaleIdentityRejection ;;
    loss-witness-timeout) module=LossEvidence; expected=NoPendingTimeout ;;
    evidence) module=Evidence ;;
    manual-reuse) module=Evidence; expected=NoCancelledReuse ;;
  esac
  log="$check_dir/$configuration.log"
  status=0
  "$java_bin" -Xmx2g -cp "$tla_jar" tlc2.TLC -workers 1 -fp 0 -seed 1 \
    -metadir "$check_dir/$configuration-states" \
    -config "$configuration.cfg" "$module.tla" > "$log" 2>&1 || status=$?
  if [ "$expected" = pass ]; then
    if [ "$status" -ne 0 ] || ! grep -q 'Model checking completed. No error has been found.' "$log"; then
      cat "$log"; exit 1
    fi
  else
    if [ "$status" -ne 12 ] || ! grep -q "^Error: Invariant $expected is violated." "$log"; then
      cat "$log"; exit 1
    fi
  fi
  counts=$(sed -n 's/^\([0-9]*\) states generated, \([0-9]*\) distinct states found,.*/\1\t\2/p' "$log" | tail -1)
  printf '%s\t%s\t%s\n' "$configuration" "$expected" "$counts" | tee -a "$check_dir/results.tsv"
done
