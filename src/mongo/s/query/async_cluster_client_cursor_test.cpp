/**
 *    Copyright 2015 MongoDB Inc.
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

#include "mongo/s/query/cluster_client_cursor.h"

#include "mongo/db/json.h"
#include "mongo/db/query/getmore_response.h"
#include "mongo/db/query/lite_parsed_query.h"
#include "mongo/db/repl/replication_executor_test_fixture.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/task_executor.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

    class AsyncClusterClientCursorTest : public repl::ReplicationExecutorTest {
    public:
        void setUp() final {
            ReplicationExecutorTest::setUp();
            launchExecutorThread();
            executor = &getExecutor();
        }

        void postExecutorThreadLaunch() final { }

    protected:
        /**
         * Given a find command specification, 'findCmd', and a list of remote host:port pairs,
         * constructs the appropriate ACCC.
         */
        void makeCursorFromFindCmd(const BSONObj& findCmd,
                                   const std::vector<HostAndPort>& remotes) {
            const bool isExplain = true;
            lpq = unittest::assertGet(LiteParsedQuery::fromFindCommand(_fullns,
                                                                       findCmd,
                                                                       isExplain));
            params = ClusterClientCursorParams(_fullns);
            params.cmdObj = findCmd;
            params.sort = lpq->getSort();
            params.projection = lpq->getProj();
            params.limit = lpq->getLimit();
            params.batchSize = lpq->getBatchSize();
            if (lpq->getSkip()) {
                params.skip = lpq->getSkip();
            }

            accc = stdx::make_unique<AsyncClusterClientCursor>(executor, params, remotes);
        }

        /**
         * Schedules a list of getMore responses to be returned by the mock network.
         */
        void scheduleNetworkResponses(std::vector<GetMoreResponse> responses) {
            std::vector<BSONObj> objs;
            for (const auto& getMoreResponse : responses) {
                objs.push_back(getMoreResponse.toBSON());
            }
            scheduleNetworkResponses(objs);
        }

        /**
         * Schedules a list of raw BSON command responses to be returned by the mock network.
         */
        void scheduleNetworkResponses(std::vector<BSONObj> objs) {
            executor::NetworkInterfaceMock* net = getNet();
            net->enterNetwork();
            for (const auto& obj : objs) {
                ASSERT_TRUE(net->hasReadyRequests());
                Milliseconds millis(0);
                RemoteCommandResponse response(obj, millis);
                repl::ReplicationExecutor::ResponseStatus responseStatus(response);
                net->scheduleResponse(net->getNextReadyRequest(), net->now(), responseStatus);
            }
            net->runReadyNetworkOperations();
            net->exitNetwork();
        }

        const std::string _fullns = "testdb.testcoll";
        const std::vector<HostAndPort> _remotes = {
            HostAndPort("localhost", -1),
            HostAndPort("localhost", -2),
            HostAndPort("localhost", -3)
        };

        executor::TaskExecutor* executor;
        std::unique_ptr<LiteParsedQuery> lpq;

        ClusterClientCursorParams params;
        std::unique_ptr<AsyncClusterClientCursor> accc;
    };

    TEST_F(AsyncClusterClientCursorTest, ClusterFind) {
        BSONObj findCmd = fromjson("{find: 'testcoll'}");
        makeCursorFromFindCmd(findCmd, _remotes);

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 1}"),
                                        fromjson("{_id: 2}"),
                                        fromjson("{_id: 3}") };
        GetMoreResponse response1(_fullns, CursorId(0), batch1);
        scheduleNetworkResponses({response1});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 1}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 2}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 3}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_FALSE(accc->ready());

        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch2 = { fromjson("{_id: 4}") };
        GetMoreResponse response2(_fullns, CursorId(0), batch2);
        std::vector<BSONObj> batch3 = { fromjson("{_id: 5}"), fromjson("{_id: 6}") };
        GetMoreResponse response3(_fullns, CursorId(0), batch3);
        scheduleNetworkResponses({response2, response3});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 4}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 5}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 6}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT(!unittest::assertGet(accc->nextReady()));
    }

    TEST_F(AsyncClusterClientCursorTest, ClusterFindAndGetMore) {
        BSONObj findCmd = fromjson("{find: 'testcoll', batchSize: 2}");
        makeCursorFromFindCmd(findCmd, _remotes);

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 1}"), fromjson("{_id: 2}") };
        GetMoreResponse response1(_fullns, CursorId(10), batch1);
        std::vector<BSONObj> batch2 = { fromjson("{_id: 3}"), fromjson("{_id: 4}") };
        GetMoreResponse response2(_fullns, CursorId(11), batch2);
        std::vector<BSONObj> batch3 = { fromjson("{_id: 5}"), fromjson("{_id: 6}") };
        GetMoreResponse response3(_fullns, CursorId(12), batch3);
        scheduleNetworkResponses({response1, response2, response3});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 1}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 2}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 3}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 4}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 5}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 6}"), *unittest::assertGet(accc->nextReady()));

        ASSERT_FALSE(accc->ready());
        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch4 = { fromjson("{_id: 7}"), fromjson("{_id: 8}") };
        GetMoreResponse response4(_fullns, CursorId(10), batch4);
        std::vector<BSONObj> batch5 = { fromjson("{_id: 9}") };
        GetMoreResponse response5(_fullns, CursorId(0), batch5);
        std::vector<BSONObj> batch6 = { fromjson("{_id: 10}") };
        GetMoreResponse response6(_fullns, CursorId(0), batch6);
        scheduleNetworkResponses({response4, response5, response6});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 10}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 7}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 8}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 9}"), *unittest::assertGet(accc->nextReady()));

        ASSERT_FALSE(accc->ready());
        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch7 = { fromjson("{_id: 11}") };
        GetMoreResponse response7(_fullns, CursorId(0), batch7);
        scheduleNetworkResponses({response7});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 11}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT(!unittest::assertGet(accc->nextReady()));
    }

    TEST_F(AsyncClusterClientCursorTest, ClusterFindSorted) {
        BSONObj findCmd = fromjson("{find: 'testcoll', sort: {_id: 1}, batchSize: 2}");
        makeCursorFromFindCmd(findCmd, _remotes);

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 5}"), fromjson("{_id: 6}") };
        GetMoreResponse response1(_fullns, CursorId(0), batch1);
        std::vector<BSONObj> batch2 = { fromjson("{_id: 3}"), fromjson("{_id: 9}") };
        GetMoreResponse response2(_fullns, CursorId(0), batch2);
        std::vector<BSONObj> batch3 = { fromjson("{_id: 4}"), fromjson("{_id: 8}") };
        GetMoreResponse response3(_fullns, CursorId(0), batch3);
        scheduleNetworkResponses({response1, response2, response3});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 3}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 4}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 5}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 6}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 8}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 9}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT(!unittest::assertGet(accc->nextReady()));
    }

    TEST_F(AsyncClusterClientCursorTest, ClusterFindAndGetMoreSorted) {
        BSONObj findCmd = fromjson("{find: 'testcoll', sort: {_id: 1}, batchSize: 2}");
        makeCursorFromFindCmd(findCmd, _remotes);

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 5}"), fromjson("{_id: 6}") };
        GetMoreResponse response1(_fullns, CursorId(1), batch1);
        std::vector<BSONObj> batch2 = { fromjson("{_id: 3}"), fromjson("{_id: 4}") };
        GetMoreResponse response2(_fullns, CursorId(0), batch2);
        std::vector<BSONObj> batch3 = { fromjson("{_id: 7}"), fromjson("{_id: 8}") };
        GetMoreResponse response3(_fullns, CursorId(2), batch3);
        scheduleNetworkResponses({response1, response2, response3});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 3}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 4}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 5}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 6}"), *unittest::assertGet(accc->nextReady()));

        ASSERT_FALSE(accc->ready());
        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch4 = { fromjson("{_id: 7}"), fromjson("{_id: 10}") };
        GetMoreResponse response4(_fullns, CursorId(0), batch4);
        scheduleNetworkResponses({response4});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 7}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 7}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 8}"), *unittest::assertGet(accc->nextReady()));

        ASSERT_FALSE(accc->ready());
        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch5 = { fromjson("{_id: 9}"), fromjson("{_id: 10}") };
        GetMoreResponse response5(_fullns, CursorId(0), batch5);
        scheduleNetworkResponses({response5});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 9}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 10}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 10}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT(!unittest::assertGet(accc->nextReady()));
    }

    TEST_F(AsyncClusterClientCursorTest, ErrorOnMismatchedCursorIds) {
        BSONObj findCmd = fromjson("{find: 'testcoll'}");
        makeCursorFromFindCmd(findCmd, {_remotes[0]});

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 1}"),
                                        fromjson("{_id: 2}"),
                                        fromjson("{_id: 3}") };
        GetMoreResponse response1(_fullns, CursorId(123), batch1);
        scheduleNetworkResponses({response1});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 1}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 2}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_TRUE(accc->ready());
        ASSERT_EQ(fromjson("{_id: 3}"), *unittest::assertGet(accc->nextReady()));
        ASSERT_FALSE(accc->ready());

        readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch2 = { fromjson("{_id: 4}"),
                                        fromjson("{_id: 5}"),
                                        fromjson("{_id: 6}") };
        GetMoreResponse response2(_fullns, CursorId(456), batch2);
        scheduleNetworkResponses({response2});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        ASSERT(!accc->nextReady().isOK());
    }

    TEST_F(AsyncClusterClientCursorTest, ErrorReceivedFromShard) {
        BSONObj findCmd = fromjson("{find: 'testcoll'}");
        makeCursorFromFindCmd(findCmd, _remotes);

        ASSERT_FALSE(accc->ready());
        auto readyEvent = unittest::assertGet(accc->nextEvent());
        ASSERT_FALSE(accc->ready());

        std::vector<BSONObj> batch1 = { fromjson("{_id: 1}"), fromjson("{_id: 2}") };
        BSONObj response1 = GetMoreResponse(_fullns, CursorId(123), batch1).toBSON();
        BSONObj response2 = fromjson("{ok: 0, code: 999, errmsg: 'bad thing happened'}");
        std::vector<BSONObj> batch3 = { fromjson("{_id: 4}"), fromjson("{_id: 5}") };
        BSONObj response3 = GetMoreResponse(_fullns, CursorId(456), batch3).toBSON();
        scheduleNetworkResponses({response1, response2, response3});
        executor->waitForEvent(readyEvent);

        ASSERT_TRUE(accc->ready());
        auto statusWithNext = accc->nextReady();
        ASSERT(!statusWithNext.isOK());
        ASSERT_EQ(statusWithNext.getStatus().code(), 999);
        ASSERT_EQ(statusWithNext.getStatus().reason(), "bad thing happened");
    }

} // namespace

} // namespace mongo
