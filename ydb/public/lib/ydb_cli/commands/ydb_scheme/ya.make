LIBRARY(ydb_cli_command_ydb_scheme)

SRCS(
    ../ydb_service_scheme.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/commands/command_base
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_scheme
)

END()
