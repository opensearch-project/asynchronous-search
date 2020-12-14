/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.async.id;

import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineException;
import org.elasticsearch.ResourceNotFoundException;

public class ExceptionTranslator {

    public static Exception translateException (Exception ex) {

        if (ex instanceof AsyncSearchStateMachineClosedException) {
            return new ResourceNotFoundException(((AsyncSearchStateMachineClosedException) ex).getEvent().asyncSearchContext()
                    .getAsyncSearchId());
        } else if (ex instanceof AsyncSearchStateMachineException) {
            return new IllegalStateException(((AsyncSearchStateMachineException) ex).getEvent().asyncSearchContext()
                    .getAsyncSearchId());
        }
        return ex;
    }


}
