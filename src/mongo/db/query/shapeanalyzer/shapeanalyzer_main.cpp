/*    Copyright 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/initializer.h"
#include "mongo/db/query/shapeanalyzer/shapeanalyzer.h"
#include "mongo/util/signal_handlers_synchronous.h"
#include "mongo/util/log.h"

namespace mongo {

    static void analyzeOneShape(ShapeAnalyzer* analyzer, const std::string& shape) {
        std::vector<std::string> shapePieces;
        boost::split(shapePieces, shape, boost::is_any_of("\t"));

        // Empty fields get converted into the empty obj, except for the namespace.
        for (size_t i = 1; i < shapePieces.size(); i++) {
            if (shapePieces[i].empty()) {
                shapePieces[i] = ShapeAnalyzer::empty;
            }
        }

        if (shapePieces.size() == 4U) {
            StatusWith<ShapeAnalysisResult> status = analyzer->analyze(shapePieces[0],
                                                                       shapePieces[1],
                                                                       shapePieces[2],
                                                                       shapePieces[3]);
            if (!status.isOK()) {
                log() << "Failed to analyze shape " << shape << " due to: " << status.getStatus();
            }
            else {
                status.getValue().log(std::cout);
            }
        }
        else {
            log() << "Did not find 4 tab-separated fields in: " << shape;
        }
    }

    static void analyzeAllShapes() {
        std::unique_ptr<ShapeAnalyzer> analyzer(new ShapeAnalyzer());

        // Read from stdin until eof.
        for (std::string line; std::getline(std::cin, line); ) {
            analyzeOneShape(analyzer.get(), line);
        }
    }

} // namespace mongo

int main( int argc, char **argv, char **envp ) {
    ::mongo::setupSynchronousSignalHandlers();
    ::mongo::runGlobalInitializersOrDie(argc, argv, envp);
    ::mongo::analyzeAllShapes();
    return 0;
}
