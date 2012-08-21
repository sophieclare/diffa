/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.kernel.config

import net.lshift.diffa.schema.environment.TestDatabaseEnvironments
import net.lshift.diffa.kernel.StoreReferenceContainer
import org.junit.{Ignore, Test, AfterClass}
import org.junit.Assert._
import org.apache.commons.lang.RandomStringUtils
import net.lshift.diffa.kernel.util.MissingObjectException

class SpacePathCacheTest {

  private val storeReferences = SpacePathCacheTest.storeReferences
  private val systemConfigStore = storeReferences.systemConfigStore

  @Test
  def shouldCreateNewTopLevelSpace {
    val topLevelSpace = RandomStringUtils.randomAlphanumeric(10)
    val newSpace = systemConfigStore.createOrUpdateSpace(topLevelSpace)

    assertTrue(systemConfigStore.doesDomainExist(topLevelSpace))
    systemConfigStore.listSpaces.contains(newSpace)

  }

  @Test
  def shouldNotAcceptInvalidTopLevelSpaceName {
    val bogusPortion = RandomStringUtils.random(10, "!@#$%^&*()/")
    val topLevelSpace = RandomStringUtils.randomAlphanumeric(10) + bogusPortion
    try {
      systemConfigStore.createOrUpdateSpace(topLevelSpace)
      fail("Using %s in a space path should have caused an error".format(bogusPortion))
    }
    catch {
      case x:ConfigValidationException => // Expected
    }
  }

  @Test
  def shouldNotAcceptInvalidSubSpaceName {

    val topLevelSpace = RandomStringUtils.randomAlphanumeric(10)
    systemConfigStore.createOrUpdateSpace(topLevelSpace)
    assertTrue(systemConfigStore.doesDomainExist(topLevelSpace))

    val bogusPortion = RandomStringUtils.random(10, "!@#$%^&*()/")

    val bogusChildSpace = topLevelSpace + "/" + RandomStringUtils.randomAlphanumeric(10) + bogusPortion

    try {
      systemConfigStore.createOrUpdateSpace(bogusChildSpace)
      fail("Using %s in a space path should have caused an error".format(bogusPortion))
    }
    catch {
      case x:ConfigValidationException => // Expected
    }
  }

  @Test
  def shouldCreateSubspaceForExistingLineage {

    val parentSpace = RandomStringUtils.randomAlphanumeric(10)
    val childSpace = parentSpace + "/" + RandomStringUtils.randomAlphanumeric(10)
    val grandChildSpace = childSpace + "/" + RandomStringUtils.randomAlphanumeric(10)
    val greatGrandChildSpace = grandChildSpace + "/" + RandomStringUtils.randomAlphanumeric(10)

    systemConfigStore.createOrUpdateSpace(parentSpace)
    assertTrue(systemConfigStore.doesDomainExist(parentSpace))

    systemConfigStore.createOrUpdateSpace(childSpace)

    // TODO This doesn't work yet
    //assertTrue(systemConfigStore.doesDomainExist(childSpace))

    systemConfigStore.createOrUpdateSpace(grandChildSpace)

    systemConfigStore.createOrUpdateSpace(greatGrandChildSpace)
  }

  @Test(expected = classOf[MissingObjectException])
  def shouldNotCreateSubspaceForNonExistentParent {

    val parentSpace = RandomStringUtils.randomAlphanumeric(10)
    assertFalse(systemConfigStore.doesDomainExist(parentSpace))

    val childSpace = parentSpace + "/" + RandomStringUtils.randomAlphanumeric(10)
    systemConfigStore.createOrUpdateSpace(childSpace)
  }

}

object SpacePathCacheTest {
  private[SpacePathCacheTest] val env =
    TestDatabaseEnvironments.uniqueEnvironment("target/domainConfigStore")

  private[SpacePathCacheTest] val storeReferences =
    StoreReferenceContainer.withCleanDatabaseEnvironment(env)

  @AfterClass
  def tearDown {
    storeReferences.tearDown
  }
}
