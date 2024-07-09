#include "service_scheme.h"

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvModifySchemeRequest = TGrpcRequestOperationCall<
    Ydb::Scheme::ModifySchemeRequest,
    Ydb::Scheme::ModifySchemeResponse
>;

class TModifySchemeRPC : public TRpcSchemeRequestActor<TModifySchemeRPC, TEvModifySchemeRequest> {
    using TBase = TRpcSchemeRequestActor<TModifySchemeRPC, TEvModifySchemeRequest>;

public:
    TModifySchemeRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TThis::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        auto proposeRequest = TBase::CreateProposeTransaction();
        auto& record = proposeRequest->Record;
        auto& transaction = *record.MutableTransaction();

        if (req->modify_schemes_size() == 0) {
            Request_->RaiseIssue(NYql::TIssue("Emply modify_schemes field"));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        std::vector<NKikimrSchemeOp::TModifyScheme> modify_schemes;
        bool parseOk = true;
        {
            NKikimrSchemeOp::TModifyScheme proto;
            for (int i = 0; i < req->modify_schemes_size() && parseOk; ++i) {
                if (parseOk = proto.ParseFromString(req->modify_schemes(i))) {
                    modify_schemes.push_back(proto);
                }
            }
        }
        if (!parseOk) {
            Request_->RaiseIssue(NYql::TIssue("Invalid modify_schemes protos"));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        for (auto& i : modify_schemes) {
            *transaction.AddTransactionalModification() = i;
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoModifySchemeRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TModifySchemeRPC(p.release()));
}

template<>
IActor* TEvModifySchemeRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TModifySchemeRPC(msg);
}

} // namespace NKikimr
} // namespace NGRpcService
