#include "prx/libc/include/general/VabiMacros.hpp"
#include "SceTypes.hpp"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" {
int APS5_VABI sceNpGetUserIdByAccountId(uint64_t accountId, int* userId);
int APS5_VABI sceNpGetNpId(int user_id, NpId* np_id);
}

namespace {

constexpr int InvalidArgument = static_cast<int>(0x80550003u);
constexpr int SignedOut = static_cast<int>(0x80550006u);

void Require(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "NpManager: %s\n", message);
        std::abort();
    }
}

}

int main() {
    int userId = 0x7a7a;
    Require(sceNpGetUserIdByAccountId(0x1234, &userId) == SignedOut, "sceNpGetUserIdByAccountId must report the account as signed out");
    Require(userId == 0x7a7a, "sceNpGetUserIdByAccountId must leave the user id untouched");
    Require(sceNpGetUserIdByAccountId(0, &userId) == InvalidArgument, "sceNpGetUserIdByAccountId must reject account id 0");
    Require(sceNpGetUserIdByAccountId(0x1234, nullptr) == InvalidArgument, "sceNpGetUserIdByAccountId must reject a null user id");
    NpId npId{};
    std::memset(&npId, 0x5a, sizeof(npId));
    NpId untouched{};
    std::memset(&untouched, 0x5a, sizeof(untouched));
    Require(sceNpGetNpId(0x10000, &npId) == SignedOut, "sceNpGetNpId must report the user as signed out");
    Require(std::memcmp(&npId, &untouched, sizeof(npId)) == 0, "sceNpGetNpId must leave the NpId untouched");
    Require(sceNpGetNpId(0x10000, nullptr) == InvalidArgument, "sceNpGetNpId must reject a null NpId");
    return 0;
}
