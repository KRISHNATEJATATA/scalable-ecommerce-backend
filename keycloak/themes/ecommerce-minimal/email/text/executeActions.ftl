<#-- Backend-minimal account set-up email (admin invite flow: POST /v1/admin/users
     -> create_user -> execute-actions email via sendExecuteActions ->
     text|html/executeActions.ftl — camelCase filename, a KC26 gotcha). Same
     copy as the verification mail; the actions are listed by Keycloak's default
     executeActions flow, so only the greeting/wrapper copy is branded here. -->
Welcome to scalable ecommerce backend.

Dear ${user.username},

to activate your account please click on this link:

${link}

This link will expire within ${linkExpirationFormatter(linkExpiration)}.

If you didn't create this account, just ignore this message.
