package net.lshift.diffa.schema.migrations.steps

import net.lshift.diffa.schema.migrations.VerifiedMigrationStep
import org.hibernate.cfg.Configuration
import net.lshift.hibernate.migrations.MigrationBuilder
import java.sql.Types
import Step0048.{createSpace,createUser}
import scala.collection.JavaConversions._
import org.hibernate.mapping.Column

/**
 * Database step adding user roles.
 */

object Step0049 extends VerifiedMigrationStep {

  def versionId = 49
  def name = "User roles"

  def createMigration(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    // NOTE: When hierarchical spaces are implemented, we'll want a slightly stronger uniqueness check to ensure that
    //       the same name isn't re-used in a space hierarchy. This may, however, not be achievable in the database
    //       layer.
    migration.createTable("member_roles").
      column("space", Types.BIGINT, false).
      column("name", Types.VARCHAR, 50, false).
      pk("space", "name")
    migration.alterTable("member_roles").
      addForeignKey("fk_role_spcs", "space", "spaces", "id")

    migration.createTable("role_permissions").
      column("space", Types.BIGINT, false).
      column("role", Types.VARCHAR, 50, false).
      column("permission", Types.VARCHAR, 50, false).
      pk("space", "role", "permission")

    migration.insert("member_roles").values(Map(
      "space" -> "0",
      "name" -> "user"
    ))
    migration.insert("role_permissions").values(Map(
      "space" -> "0",
      "role" -> "Admin",
      "permission" -> "domain-user"
    ))

    // We can no longer create a foreign key based purely upon the user being a member of the space. Instead, just
    // ensure the user exists.
    migration.alterTable("user_item_visibility").
      dropForeignKey("fk_uiv_mmbs").
      addForeignKey("fk_uiv_user", Array("username"), "users", Array("name"))

    // NOTE: When hierarchical spaces are implemented, this membership may also need to reference the space where
    //       the role was defined. This will allow a role to be defined at a high level, and then be used in applied
    //       to specific child spaces (ie, define a top level "Admin" role, but then only apply it to a specific subspace).
    migration.alterTable("members").
      addColumn("role", Types.VARCHAR, 50, false, "Admin").
      addColumn("role_space", Types.BIGINT, Column.DEFAULT_LENGTH, false, 0).
      dropPrimaryKey().
      addPrimaryKey("space", "username", "role_space", "role").
      addForeignKey("fk_mmbs_role", Array("role_space", "role"), "member_roles", Array("space", "name"))

    migration
  }

  def applyVerification(config: Configuration) = {
    val migration = new MigrationBuilder(config)

    val spaceName = randomString()
    val spaceId = randomInt()

    createSpace(migration, spaceId, spaceName)

    val user = randomString()
    val role = randomString()
    val permission1 = randomString()
    val permission2 = randomString()

    createUser(migration, user)
    createRole(migration, spaceId, role, permission1, permission2)
    createMember(migration, spaceId, user, spaceId, role)

    migration
  }

  def createRole(migration:MigrationBuilder, spaceId:String, name:String, permissions:String*) {
    migration.insert("member_roles").values(Map(
      "space" -> spaceId,
      "name" -> name
    ))

    permissions.foreach(p =>
      migration.insert("role_permissions").values(Map(
        "space" -> spaceId,
        "role" -> name,
        "permission" -> p
      ))
    )
  }

  def createMember(migration:MigrationBuilder, spaceId:String, user:String, roleSpaceId:String, role:String) {
    migration.insert("members").values(Map(
      "space" -> spaceId,
      "username" -> user,
      "role_space" -> roleSpaceId,
      "role" -> role
    ))
  }
}