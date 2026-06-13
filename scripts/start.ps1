param(
  [int]$Port = 8771
)

$env:ERL_FLAGS = ""
rebar3 shell --eval "application:set_env(arbiguard,http_port,$Port)."
