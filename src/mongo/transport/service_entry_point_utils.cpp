/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/transport/service_entry_point_utils.h"

#include <fmt/format.h>
#include <functional>
#include <memory>

#include "mongo/logv2/log.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/thread_safety_context.h"

#if !defined(_WIN32)
#include <sys/resource.h>
#endif

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

namespace mongo {

namespace {
void* runFunc(void* ctx) {
    std::unique_ptr<std::function<void()>> taskPtr(static_cast<std::function<void()>*>(ctx));
    (*taskPtr)();

    return nullptr;
}
}  // namespace

Status launchServiceWorkerThread(std::function<void()> task) noexcept {

    try {
#if defined(_WIN32)
        stdx::thread(std::move(task)).detach();
#else
        pthread_attr_t attrs;
        pthread_attr_init(&attrs);
        pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

        static const rlim_t kStackSize =
            1024 * 1024;  // if we change this we need to update the warning

        struct rlimit limits;
        invariant(getrlimit(RLIMIT_STACK, &limits) == 0);
        if (limits.rlim_cur > kStackSize) {
            size_t stackSizeToSet = kStackSize;
#if !__has_feature(address_sanitizer)
            if (kDebugBuild)
                stackSizeToSet /= 2;
#endif
            int failed = pthread_attr_setstacksize(&attrs, stackSizeToSet);
            if (failed) {
                const auto ewd = errnoWithDescription(failed);
                LOGV2_WARNING(22949,
                              "pthread_attr_setstacksize failed: {error}",
                              "pthread_attr_setstacksize failed",
                              "error"_attr = ewd);
            }
        } else if (limits.rlim_cur < 1024 * 1024) {
            LOGV2_WARNING(22950,
                          "Stack size set to {stackSizeKiB}KiB. We suggest 1024KiB",
                          "Stack size not set to suggested 1024KiB",
                          "stackSizeKiB"_attr = (limits.rlim_cur / 1024));
        }

        // Wrap the user-specified `task` so it runs with an installed `sigaltstack`.
        task = [sigAltStackController = std::make_shared<stdx::support::SigAltStackController>(),
                f = std::move(task)] {
            auto sigAltStackGuard = sigAltStackController->makeInstallGuard();
            f();
        };

        pthread_t thread;
        auto ctx = std::make_unique<std::function<void()>>(std::move(task));
        ThreadSafetyContext::getThreadSafetyContext()->onThreadCreate();
        int failed = pthread_create(&thread, &attrs, runFunc, ctx.get());

        pthread_attr_destroy(&attrs);

        if (failed) {
            using namespace fmt::literals;
            uassert(
                4850900, "pthread_create failed: {}"_format(errnoWithDescription(failed)), failed);
            throw std::system_error(
                std::make_error_code(std::errc::resource_unavailable_try_again));
        }

        ctx.release();
#endif

    } catch (const std::exception& e) {
        LOGV2_ERROR(22948, "pthread_create failed: {errno}", "pthread_create failed");
        return {ErrorCodes::InternalError,
                str::stream() << "Failed to create service entry worker thread: " << e.what()};
    }

    return Status::OK();
}

}  // namespace mongo
