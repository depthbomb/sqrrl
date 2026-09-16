from sqrrl.schema import Index, Schema, Table, boolean, integer, text

schema = Schema(
        tables=(
            Table(
                    "users",
                    model="User",
                    fields=(
                        integer("id").primary_key(),
                        text("name"),
                        text("email").nullable().unique(),
                    ),
            ),
            Table(
                    "tasks",
                    model="Task",
                    fields=(
                        integer("id").primary_key(),
                        integer("owner_id").references("users", "id", on_delete="CASCADE"),
                        text("title"),
                        boolean("done").default("0"),
                    ),
                    indexes=(Index("tasks_owner_idx", ("owner_id",)),),
            ),
            Table(
                    "settings",
                    model="Setting",
                    primary_key=("user_id", "key"),
                    fields=(
                        integer("user_id").references("users", "id", on_delete="CASCADE"),
                        text("key"),
                        text("value").nullable(),
                    ),
            ),
        )
)
