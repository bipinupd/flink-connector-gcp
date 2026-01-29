/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.flink.connector.gcp.bigtable.utils;

import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link BigtableUtils} class.
 *
 * <p>This class verifies the functionality of the {@link BigtableUtils} by testing its ability to
 * perform various utility operations related to Bigtable, such as checking for the existence of
 * column families and validating table identifiers.
 */
public class BigtableUtilsTest {

    @Test
    public void testIsColumnFamilyInexistent() {
        assertTrue(BigtableUtils.isColumnFamilyInexistent("unknown family"));
    }

    @Test
    public void testIsColumnFamilyInexistentWrongError() {
        assertFalse(BigtableUtils.isColumnFamilyInexistent("other error"));
    }

    @Test
    public void testValidateTableIdentifier() {
        BigtableUtils.validateTableIdentifier(
                TestingUtils.PROJECT, TestingUtils.INSTANCE, TestingUtils.TABLE);
    }

    @Test
    public void testValidateTableIdentifierNullProject() {
        assertThrows(
                NullPointerException.class,
                () ->
                        BigtableUtils.validateTableIdentifier(
                                null, TestingUtils.INSTANCE, TestingUtils.TABLE));
    }

    @Test
    public void testValidateTableIdentifierNullInstance() {
        assertThrows(
                NullPointerException.class,
                () ->
                        BigtableUtils.validateTableIdentifier(
                                TestingUtils.PROJECT, null, TestingUtils.TABLE));
    }

    @Test
    public void testValidateTableIdentifierNullTable() {
        assertThrows(
                NullPointerException.class,
                () ->
                        BigtableUtils.validateTableIdentifier(
                                TestingUtils.PROJECT, TestingUtils.INSTANCE, null));
    }
}
