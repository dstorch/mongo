/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <memory>
#include <vector>

#include "mongo/executor/network_interface.h"
#include "mongo/executor/network_interface_asio.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {
namespace executor {

class NetworkInterfaceASIOPartitioned final : public NetworkInterface {
public:
    struct Options {
        Options() = default;

// Explicit move construction and assignment to support MSVC
#if defined(_MSC_VER) && _MSC_VER < 1900
        Options(Options&&);
        Options& operator=(Options&&);
#else
        Options(Options&&) = default;
        Options& operator=(Options&&) = default;
#endif

        ConnectionPool::Options connectionPoolOptions;
        std::unique_ptr<AsyncTimerFactoryInterface> timerFactory;
        std::unique_ptr<NetworkConnectionHook> networkConnectionHook;
        std::unique_ptr<AsyncStreamFactoryInterface> streamFactory;
        std::unique_ptr<rpc::EgressMetadataHook> metadataHook;
    };

    NetworkInterfaceASIOPartitioned(size_t numUnderlyingNets, Options = Options());

    std::string getDiagnosticString() override;
    void appendConnectionStats(BSONObjBuilder* b) override;
    std::string getHostName() override;
    void startup() override;
    void shutdown() override;
    void waitForWork() override;
    void waitForWorkUntil(Date_t when) override;
    void signalWorkAvailable() override;
    Date_t now() override;
    void startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                      const RemoteCommandRequest& request,
                      const RemoteCommandCompletionFn& onFinish) override;
    void cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) override;
    void cancelAllCommands() override;
    void setAlarm(Date_t when, const stdx::function<void()>& action) override;

private:
    NetworkInterfaceASIO* getNextNet();

    Options _options;

    std::vector<std::unique_ptr<NetworkInterfaceASIO>> _nets;

    // Operations get round-robined to each of the underlying network interfaces in '_nets'.
    AtomicInt64 _roundRobinCounter;
};

}  // namespace executor
}  // namespace mongo
