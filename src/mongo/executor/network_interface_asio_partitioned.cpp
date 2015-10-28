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

#include "mongo/platform/basic.h"

#include "mongo/executor/network_interface_asio_partitioned.h"

#include "mongo/executor/async_stream_interface.h"
#include "mongo/stdx/memory.h"

#define FORWARD_TO_ALL_NETS(FUNC, ...) \
    for (auto& net : _nets) {          \
        net->FUNC(__VA_ARGS__);        \
    }

namespace mongo {
namespace executor {

NetworkInterfaceASIOPartitioned::NetworkInterfaceASIOPartitioned(size_t numUnderlyingNets,
                                                                 Options options)
    : _options(std::move(options)) {
    _nets.resize(numUnderlyingNets);
    for (size_t i = 0; i < numUnderlyingNets; ++i) {
        NetworkInterfaceASIO::Options underlyingOptions;
        underlyingOptions.connectionPoolOptions = _options.connectionPoolOptions;
        underlyingOptions.timerFactory = _options.timerFactory.get();
        underlyingOptions.networkConnectionHook = _options.networkConnectionHook.get();
        underlyingOptions.streamFactory = _options.streamFactory.get();
        underlyingOptions.metadataHook = _options.metadataHook.get();
        _nets[i] = stdx::make_unique<NetworkInterfaceASIO>(std::move(underlyingOptions));
    }
}

std::string NetworkInterfaceASIOPartitioned::getDiagnosticString() {
    // TODO: Combine diagnostic strings from all nets?
    return getNextNet()->getDiagnosticString();
}

void NetworkInterfaceASIOPartitioned::appendConnectionStats(BSONObjBuilder* b) {
    // TODO: Combine stats from all nets?
    getNextNet()->appendConnectionStats(b);
}

std::string NetworkInterfaceASIOPartitioned::getHostName() {
    return getNextNet()->getHostName();
}

void NetworkInterfaceASIOPartitioned::startup() {
    FORWARD_TO_ALL_NETS(startup);
}

void NetworkInterfaceASIOPartitioned::shutdown() {
    FORWARD_TO_ALL_NETS(shutdown);
}

void NetworkInterfaceASIOPartitioned::waitForWork() {
    // TODO: This seems wrong.
    FORWARD_TO_ALL_NETS(waitForWork);
}

void NetworkInterfaceASIOPartitioned::waitForWorkUntil(Date_t when) {
    // TODO: This seems wrong.
    FORWARD_TO_ALL_NETS(waitForWorkUntil, when);
}

void NetworkInterfaceASIOPartitioned::signalWorkAvailable() {
    // TODO: This seems wrong.
    FORWARD_TO_ALL_NETS(signalWorkAvailable);
}

Date_t NetworkInterfaceASIOPartitioned::now() {
    return Date_t::now();
}

void NetworkInterfaceASIOPartitioned::startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                                                   const RemoteCommandRequest& request,
                                                   const RemoteCommandCompletionFn& onFinish) {
    getNextNet()->startCommand(cbHandle, request, onFinish);
}

void NetworkInterfaceASIOPartitioned::cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) {
    // We don't know which underlying network interface is handling this request, so just forward to
    // everyone. This should be a no-op for all network interfaces that don't know about this
    // 'cbHandle'.
    FORWARD_TO_ALL_NETS(cancelCommand, cbHandle);
}

void NetworkInterfaceASIOPartitioned::cancelAllCommands() {
    FORWARD_TO_ALL_NETS(cancelAllCommands);
}

void NetworkInterfaceASIOPartitioned::setAlarm(Date_t when, const stdx::function<void()>& action) {
    getNextNet()->setAlarm(when, action);
}

NetworkInterfaceASIO* NetworkInterfaceASIOPartitioned::getNextNet() {
    uint64_t counter = _roundRobinCounter.fetchAndAdd(1);
    return _nets[counter % _nets.size()].get();
}

}  // namespace executor
}  // namespace mongo
