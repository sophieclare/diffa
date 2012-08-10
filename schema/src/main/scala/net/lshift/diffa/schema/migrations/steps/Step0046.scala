/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.schema.migrations.steps

import org.hibernate.cfg.Configuration
import net.lshift.diffa.schema.migrations.VerifiedMigrationStep
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types
import scala.collection.JavaConversions._

object Step0046 extends VerifiedMigrationStep {

  def versionId = 46
  def name = "Introduce sub spaces"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    migration.createTable("spaces").
      column("id", Types.INTEGER, false).
      column("name", Types.VARCHAR, 50, false).
      column("config_version", Types.INTEGER, 11, false, 0).
      pk("id")

    migration.createTable("space_paths").
      column("parent", Types.INTEGER, false).
      column("child", Types.INTEGER, false).
      column("depth", Types.INTEGER, false).
      pk("parent", "child")

    migration.alterTable("space_paths").addForeignKey("fk_space_par", "parent", "spaces", "id")
    migration.alterTable("space_paths").addForeignKey("fk_space_chd", "child", "spaces", "id")

    migration
  }

  def applyVerification(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    val parentId = randomInt()
    val childId = randomInt()
    val parentName = randomString()
    val childName = randomString()

    migration.insert("spaces").values(Map(
      "id"                -> parentId,
      "name"              -> parentName,
      "config_version"    -> "0"
    ))

    migration.insert("space_paths").values(Map(
      "parent"  -> parentId,
      "child"   -> parentId,
      "depth"   -> "0"
    ))

    migration.insert("spaces").values(Map(
      "id"                -> childId,
      "name"              -> childName,
      "config_version"    -> "0"
    ))

    migration.insert("space_paths").values(Map(
      "parent"  -> childId,
      "child"   -> childId,
      "depth"   -> "0"
    ))

    migration.insert("space_paths").values(Map(
      "parent"  -> parentId,
      "child"   -> childId,
      "depth"   -> "1"
    ))

    migration
  }
}
