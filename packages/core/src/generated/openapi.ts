export interface paths {
    "/api/v1/accounts": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Accounts
         * @description List accounts.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Account service.
         *         actor: Caller context.
         *         params: Account list params.
         *
         *     Returns:
         *         Page of accounts.
         */
        get: operations["list_accounts_api_v1_accounts_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/accounts/me": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Current Account
         * @description Get the calling account.
         *
         *     Clients observe HTTP 200 on success.
         *
         *     Args:
         *         actor: Caller context.
         *
         *     Returns:
         *         Calling account.
         */
        get: operations["get_current_account_api_v1_accounts_me_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/accounts/{account_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Account
         * @description Get an account by id.
         *
         *     Clients observe HTTP 200 on success and 404 when the account does not
         *     exist.
         *
         *     Args:
         *         account_id: Id of the account.
         *         service: Account service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored account.
         */
        get: operations["get_account_api_v1_accounts__account_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/agent-versions/{agent_version_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Agent Version
         * @description Get an agent version by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no agent version has
         *     this id.
         *
         *     Args:
         *         agent_version_id: Id of the agent version.
         *         service: Agent version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored agent version.
         */
        get: operations["get_agent_version_api_v1_agent_versions__agent_version_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Agent Version
         * @description Delete an agent version.
         *
         *     Clients observe HTTP 204 on success, 404 when no agent version has
         *     this id, and 409 when an experiment run references it.
         *
         *     Args:
         *         agent_version_id: Id of the agent version.
         *         service: Agent version service.
         *         actor: Caller context.
         */
        delete: operations["delete_agent_version_api_v1_agent_versions__agent_version_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Agent Version
         * @description Update an agent version.
         *
         *     Clients observe HTTP 200 on success, 404 when no agent version has this
         *     id, and 422 on invalid input.
         *
         *     Args:
         *         agent_version_id: Id of the agent version.
         *         body: Agent version update request.
         *         service: Agent version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated agent version.
         */
        patch: operations["update_agent_version_api_v1_agent_versions__agent_version_id__patch"];
        trace?: never;
    };
    "/api/v1/agents": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Agents
         * @description List agents.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Agent service.
         *         actor: Caller context.
         *         params: Agent list params.
         *
         *     Returns:
         *         Page of agents.
         */
        get: operations["list_agents_api_v1_agents_get"];
        put?: never;
        /**
         * Create Agent
         * @description Create an agent.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input.
         *
         *     Args:
         *         body: Agent create request.
         *         service: Agent service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created agent.
         */
        post: operations["create_agent_api_v1_agents_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/agents/{agent_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Agent
         * @description Get an agent by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no agent has this id.
         *
         *     Args:
         *         agent_id: Id of the agent.
         *         service: Agent service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored agent.
         */
        get: operations["get_agent_api_v1_agents__agent_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Agent
         * @description Delete an agent, hiding the agent and retaining its subtree.
         *
         *     The agent's stored sessions, versions, cohorts, experiments, and
         *     investigations are retained and stay readable through their own routes.
         *     Creating new ones for the agent returns HTTP 404. Clients observe HTTP
         *     204 on success and 404 when no agent has this id.
         *
         *     Args:
         *         agent_id: Id of the agent.
         *         service: Agent service.
         *         actor: Caller context.
         */
        delete: operations["delete_agent_api_v1_agents__agent_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Agent
         * @description Update an agent.
         *
         *     Clients observe HTTP 200 on success, 404 when no agent has this id, and
         *     422 on invalid input, including an attempt to clear the name.
         *
         *     Args:
         *         agent_id: Id of the agent.
         *         body: Agent update request.
         *         service: Agent service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated agent.
         */
        patch: operations["update_agent_api_v1_agents__agent_id__patch"];
        trace?: never;
    };
    "/api/v1/agents/{agent_id}/versions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Agent Versions
         * @description List the versions of an agent.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         agent_id: Id of the agent.
         *         service: Agent version service.
         *         actor: Caller context.
         *         params: Agent version list params.
         *
         *     Returns:
         *         Page of agent versions.
         */
        get: operations["list_agent_versions_api_v1_agents__agent_id__versions_get"];
        put?: never;
        /**
         * Create Agent Version
         * @description Create a new version of an agent.
         *
         *     Clients observe HTTP 201 on success, 404 when no agent has this id, and
         *     422 on invalid input.
         *
         *     Args:
         *         agent_id: Id of the agent.
         *         body: Agent version create request.
         *         service: Agent version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created agent version.
         */
        post: operations["create_agent_version_api_v1_agents__agent_id__versions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/annotations": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Annotations
         * @description List annotations.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Annotation service.
         *         actor: Caller context.
         *         params: Annotation list params.
         *
         *     Returns:
         *         Page of annotations.
         */
        get: operations["list_annotations_api_v1_annotations_get"];
        put?: never;
        /**
         * Create Annotation
         * @description Create a manual annotation, or answer an investigation session.
         *
         *     A body carrying session_id creates a manual annotation. A body carrying
         *     investigation_session_id answers an investigation session, moving a
         *     pending investigation to in_progress on its first answer.
         *
         *     Clients observe HTTP 201 on success, 404 when the session or the
         *     investigation session does not exist, and 422 when the selector names a
         *     node outside the session.
         *
         *     Args:
         *         body: Manual annotation or investigation answer create request.
         *         service: Annotation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created annotation.
         */
        post: operations["create_annotation_api_v1_annotations_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/annotations/{annotation_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Annotation
         * @description Get an annotation by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no annotation has this
         *     id.
         *
         *     Args:
         *         annotation_id: Id of the annotation.
         *         service: Annotation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored annotation.
         */
        get: operations["get_annotation_api_v1_annotations__annotation_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Annotation
         * @description Delete an annotation.
         *
         *     Clients observe HTTP 204 on success and 404 when no annotation has this
         *     id.
         *
         *     Args:
         *         annotation_id: Id of the annotation.
         *         service: Annotation service.
         *         actor: Caller context.
         */
        delete: operations["delete_annotation_api_v1_annotations__annotation_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Annotation
         * @description Set a new value on an annotation.
         *
         *     Clients observe HTTP 200 on success and 404 when no annotation has this
         *     id.
         *
         *     Args:
         *         annotation_id: Id of the annotation.
         *         body: Annotation update request.
         *         service: Annotation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated annotation.
         */
        patch: operations["update_annotation_api_v1_annotations__annotation_id__patch"];
        trace?: never;
    };
    "/api/v1/api-keys": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Api Keys
         * @description List API keys of the caller.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: API key service.
         *         actor: Caller context.
         *         params: API key list params.
         *
         *     Returns:
         *         Page of API keys.
         */
        get: operations["list_api_keys_api_v1_api_keys_get"];
        put?: never;
        /**
         * Create Api Key
         * @description Create an API key.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input. The response carries the plaintext
         *     key exactly once.
         *
         *     Args:
         *         body: API key create request.
         *         service: API key service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created API key including the plaintext key.
         */
        post: operations["create_api_key_api_v1_api_keys_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/api-keys/{api_key_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Api Key
         * @description Get an API key by id.
         *
         *     Clients observe HTTP 200 on success and 404 when the caller owns no api
         *     key with this id.
         *
         *     Args:
         *         api_key_id: Id of the API key.
         *         service: API key service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored API key.
         */
        get: operations["get_api_key_api_v1_api_keys__api_key_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Api Key
         * @description Delete an API key.
         *
         *     Clients observe HTTP 204 on success and 404 when the caller owns no api
         *     key with this id.
         *
         *     Args:
         *         api_key_id: Id of the API key.
         *         service: API key service.
         *         actor: Caller context.
         */
        delete: operations["delete_api_key_api_v1_api_keys__api_key_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Api Key
         * @description Update an API key.
         *
         *     Clients observe HTTP 200 on success, 404 when the caller owns no API key
         *     with this id, and 422 on invalid input.
         *
         *     Args:
         *         api_key_id: Id of the API key.
         *         body: API key update request.
         *         service: API key service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated API key.
         */
        patch: operations["update_api_key_api_v1_api_keys__api_key_id__patch"];
        trace?: never;
    };
    "/api/v1/api-keys/{api_key_id}/rotate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Rotate Api Key
         * @description Rotate an API key.
         *
         *     Clients observe HTTP 200 on success, 404 when the caller owns no API key
         *     with this id, and 422 on invalid input. The response carries the new
         *     plaintext key exactly once.
         *
         *     Args:
         *         api_key_id: Id of the API key.
         *         body: API key rotate request.
         *         service: API key service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Rotated API key including the new plaintext key.
         */
        post: operations["rotate_api_key_api_v1_api_keys__api_key_id__rotate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/blobs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Upload Blob
         * @description Upload a blob, deduping identical content by sha256.
         *
         *     Clients observe HTTP 201 for a new blob, 200 on a dedup hit, and 413
         *     when the upload exceeds the size cap.
         *
         *     Args:
         *         response: Response, its status code is set to 201 or 200.
         *         file: Uploaded file.
         *         service: Blob service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored blob metadata.
         */
        post: operations["upload_blob_api_v1_blobs_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/blobs/{blob_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Blob
         * @description Get a blob's metadata by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no blob has this id.
         *
         *     Args:
         *         blob_id: Id of the blob.
         *         service: Blob service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored blob metadata.
         */
        get: operations["get_blob_api_v1_blobs__blob_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Blob
         * @description Delete a blob.
         *
         *     Clients observe HTTP 204 on success, 404 when no blob has this id, and
         *     409 when the blob is referenced by a plugin version.
         *
         *     Args:
         *         blob_id: Id of the blob.
         *         service: Blob service.
         *         actor: Caller context.
         */
        delete: operations["delete_blob_api_v1_blobs__blob_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/blobs/{blob_id}/content": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Download Blob
         * @description Download a blob's raw content.
         *
         *     Clients observe HTTP 200 with the blob's media type and 404 when no
         *     blob has this id. The response never renders inline: it carries
         *     ``Content-Disposition: attachment`` and ``X-Content-Type-Options:
         *     nosniff``.
         *
         *     Args:
         *         blob_id: Id of the blob.
         *         service: Blob service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Raw blob content.
         */
        get: operations["download_blob_api_v1_blobs__blob_id__content_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/cohort-versions/{cohort_version_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Cohort Version
         * @description Get a cohort version by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no cohort version has
         *     this id.
         *
         *     Args:
         *         cohort_version_id: Id of the cohort version.
         *         service: Cohort version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored cohort version.
         */
        get: operations["get_cohort_version_api_v1_cohort_versions__cohort_version_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Cohort Version
         * @description Delete a cohort version.
         *
         *     Clients observe HTTP 204 on success and 404 when no cohort version has
         *     this id.
         *
         *     Args:
         *         cohort_version_id: Id of the cohort version.
         *         service: Cohort version service.
         *         actor: Caller context.
         */
        delete: operations["delete_cohort_version_api_v1_cohort_versions__cohort_version_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Cohort Version
         * @description Update a cohort version.
         *
         *     Clients observe HTTP 200 on success, 404 when no cohort version has this
         *     id, and 422 on invalid input.
         *
         *     Args:
         *         cohort_version_id: Id of the cohort version.
         *         body: Cohort version update request.
         *         service: Cohort version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated cohort version.
         */
        patch: operations["update_cohort_version_api_v1_cohort_versions__cohort_version_id__patch"];
        trace?: never;
    };
    "/api/v1/cohorts": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Cohorts
         * @description List cohorts.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Cohort service.
         *         actor: Caller context.
         *         params: Cohort list params.
         *
         *     Returns:
         *         Page of cohorts.
         */
        get: operations["list_cohorts_api_v1_cohorts_get"];
        put?: never;
        /**
         * Create Cohort
         * @description Create a cohort namespace.
         *
         *     Clients observe HTTP 201 on success, 404 when the agent does not exist,
         *     409 when the cohort name is already registered, and 422 on invalid
         *     input.
         *
         *     Args:
         *         body: Cohort create request.
         *         service: Cohort service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created cohort.
         */
        post: operations["create_cohort_api_v1_cohorts_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/cohorts/{cohort_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Cohort
         * @description Get a cohort by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no cohort has this id.
         *
         *     Args:
         *         cohort_id: Id of the cohort.
         *         service: Cohort service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored cohort.
         */
        get: operations["get_cohort_api_v1_cohorts__cohort_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Cohort
         * @description Delete a cohort.
         *
         *     Deleting a cohort cascades its versions. Clients observe HTTP 204 on
         *     success, 404 when no cohort has this id, and 409 when an experiment
         *     run references one of its versions.
         *
         *     Args:
         *         cohort_id: Id of the cohort.
         *         service: Cohort service.
         *         actor: Caller context.
         */
        delete: operations["delete_cohort_api_v1_cohorts__cohort_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Cohort
         * @description Update a cohort's name, description, and metadata.
         *
         *     Clients observe HTTP 200 on success, 404 when no cohort has this id,
         *     409 when the new name is already registered, and 422 when the update
         *     clears the name.
         *
         *     Args:
         *         cohort_id: Id of the cohort.
         *         body: Cohort update request.
         *         service: Cohort service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated cohort.
         */
        patch: operations["update_cohort_api_v1_cohorts__cohort_id__patch"];
        trace?: never;
    };
    "/api/v1/cohorts/{cohort_id}/versions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Cohort Versions
         * @description List the versions of a cohort.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         cohort_id: Id of the cohort.
         *         service: Cohort version service.
         *         actor: Caller context.
         *         params: Cohort version list params.
         *
         *     Returns:
         *         Page of cohort versions.
         */
        get: operations["list_cohort_versions_api_v1_cohorts__cohort_id__versions_get"];
        put?: never;
        /**
         * Create Cohort Version
         * @description Create a new version of a cohort from a membership delta.
         *
         *     Clients observe HTTP 201 on success, 404 when no cohort has this id or
         *     no cohort version has the baseline id, and 422 when the baseline belongs
         *     to a different cohort, the delta removes a session absent from the base
         *     version, adds a session already present, repeats a session id, or an
         *     added session is missing or belongs to a different agent.
         *
         *     Args:
         *         cohort_id: Id of the cohort.
         *         body: Cohort version create request.
         *         service: Cohort version service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created cohort version.
         */
        post: operations["create_cohort_version_api_v1_cohorts__cohort_id__versions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/device_authorization": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Device Authorization
         * @description Start a device authorization and receive its codes.
         *
         *     The caller shows the user code to a person, who confirms it at the
         *     verification URI while signed in. The caller then polls ``/api/v1/login`` with
         *     the device grant type until the confirmation lands. Clients observe HTTP
         *     200 on success and 400 when this server does not authenticate requests.
         *     The codes are returned exactly once.
         *
         *     Args:
         *         request: Incoming request.
         *         settings: Service settings governing auth behavior.
         *         service: Device service.
         *         hostname: Host the caller runs on.
         *         os: Operating system the caller runs on.
         *         python_version: Python version the caller runs.
         *         client_version: Kitaru version the caller runs.
         *
         *     Raises:
         *         TokenGrantError: This server does not authenticate requests.
         *
         *     Returns:
         *         Device authorization carrying the plaintext codes.
         */
        post: operations["device_authorization_api_v1_device_authorization_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/devices": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Devices
         * @description List devices of the caller.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Device service.
         *         actor: Caller context.
         *         params: Device list params.
         *
         *     Returns:
         *         Page of devices.
         */
        get: operations["list_devices_api_v1_devices_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/devices/{device_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Device
         * @description Get a device by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no device has this id,
         *     another account already approved it, or the user code of an unapproved
         *     device is missing or wrong.
         *
         *     Args:
         *         device_id: Id of the device.
         *         service: Device service.
         *         actor: Caller context.
         *         user_code: User code of a device no account approved yet.
         *
         *     Returns:
         *         Stored device.
         */
        get: operations["get_device_api_v1_devices__device_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Device
         * @description Delete a device.
         *
         *     Every token issued for the device stops authenticating. Clients observe
         *     HTTP 204 on success and 404 when the caller owns no device with this id.
         *
         *     Args:
         *         device_id: Id of the device.
         *         service: Device service.
         *         actor: Caller context.
         */
        delete: operations["delete_device_api_v1_devices__device_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Device
         * @description Update a device.
         *
         *     Locking a device rejects every token issued for it. Clients observe HTTP
         *     200 on success, 404 when the caller owns no device with this id, and 422 on
         *     invalid input.
         *
         *     Args:
         *         device_id: Id of the device.
         *         body: Device update request.
         *         service: Device service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated device.
         */
        patch: operations["update_device_api_v1_devices__device_id__patch"];
        trace?: never;
    };
    "/api/v1/devices/{device_id}/verify": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Verify Device
         * @description Approve a pending device authorization.
         *
         *     The device polling the token endpoint receives a token for the caller's
         *     account on its next poll. Clients observe HTTP 200 on success, 404 when no
         *     such device exists or another account already approved it, and 422 when
         *     the user code does not match, the authorization expired, or the device is
         *     locked. Three failed attempts lock the device.
         *
         *     Args:
         *         device_id: Id of the device.
         *         body: Device verify request.
         *         service: Device service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Verified device.
         */
        post: operations["verify_device_api_v1_devices__device_id__verify_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/evaluations": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Evaluations
         * @description List evaluations.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Evaluation service.
         *         actor: Caller context.
         *         params: Evaluation list params.
         *
         *     Returns:
         *         Page of evaluations.
         */
        get: operations["list_evaluations_api_v1_evaluations_get"];
        put?: never;
        /**
         * Create Evaluations
         * @description Score every input session with every evaluator, as one job.
         *
         *     Clients observe HTTP 201 on success, 404 when an evaluator or version
         *     does not exist, 409 when an input session is not finished, and 422 when
         *     the pair count exceeds the cap or an input session does not exist.
         *
         *     Args:
         *         body: Evaluation batch create request.
         *         service: Job service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created job.
         */
        post: operations["create_evaluations_api_v1_evaluations_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/evaluations/{evaluation_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Evaluation
         * @description Get an evaluation by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no evaluation has this
         *     id.
         *
         *     Args:
         *         evaluation_id: Id of the evaluation.
         *         service: Evaluation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored evaluation.
         */
        get: operations["get_evaluation_api_v1_evaluations__evaluation_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/evaluators": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Evaluators
         * @description List evaluators.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Evaluator service.
         *         actor: Caller context.
         *         params: Evaluator list params.
         *
         *     Returns:
         *         Page of evaluators.
         */
        get: operations["list_evaluators_api_v1_evaluators_get"];
        put?: never;
        /**
         * Create Evaluator
         * @description Create an evaluator.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input.
         *
         *     Args:
         *         body: Evaluator create request.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created evaluator.
         */
        post: operations["create_evaluator_api_v1_evaluators_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/evaluators/{evaluator_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Evaluator
         * @description Get an evaluator by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no evaluator has this
         *     id.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored evaluator.
         */
        get: operations["get_evaluator_api_v1_evaluators__evaluator_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Evaluator
         * @description Delete an evaluator, cascading its versions.
         *
         *     Clients observe HTTP 204 on success and 404 when no evaluator has this
         *     id.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         service: Evaluator service.
         *         actor: Caller context.
         */
        delete: operations["delete_evaluator_api_v1_evaluators__evaluator_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Evaluator
         * @description Update an evaluator.
         *
         *     Clients observe HTTP 200 on success, 404 when no evaluator has this id,
         *     and 422 on invalid input.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         body: Evaluator update request.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated evaluator.
         */
        patch: operations["update_evaluator_api_v1_evaluators__evaluator_id__patch"];
        trace?: never;
    };
    "/api/v1/evaluators/{evaluator_id}/versions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Evaluator Versions
         * @description List an evaluator's versions.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         service: Evaluator service.
         *         actor: Caller context.
         *         params: List params.
         *
         *     Returns:
         *         Page of evaluator versions.
         */
        get: operations["list_evaluator_versions_api_v1_evaluators__evaluator_id__versions_get"];
        put?: never;
        /**
         * Create Evaluator Version
         * @description Create an evaluator version.
         *
         *     Clients observe HTTP 201 on success, 404 when no evaluator has this id
         *     or a script source names an unknown blob, and 422 on invalid input.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         body: Evaluator version create request.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created evaluator version.
         */
        post: operations["create_evaluator_version_api_v1_evaluators__evaluator_id__versions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/evaluators/{evaluator_id}/versions/{version}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Evaluator Version
         * @description Get an evaluator version by version number.
         *
         *     Clients observe HTTP 200 on success and 404 when no version with this
         *     number exists for this evaluator.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         version: Version number.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored evaluator version.
         */
        get: operations["get_evaluator_version_api_v1_evaluators__evaluator_id__versions__version__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update Evaluator Version
         * @description Update an evaluator version's display version.
         *
         *     Clients observe HTTP 200 on success and 404 when no version with this
         *     number exists for this evaluator.
         *
         *     Args:
         *         evaluator_id: Id of the evaluator.
         *         version: Version number.
         *         body: Evaluator version update request.
         *         service: Evaluator service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated evaluator version.
         */
        patch: operations["update_evaluator_version_api_v1_evaluators__evaluator_id__versions__version__patch"];
        trace?: never;
    };
    "/api/v1/experiment-runs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Experiment Runs
         * @description List experiment runs.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Experiment run service.
         *         actor: Caller context.
         *         params: Experiment run list params.
         *
         *     Returns:
         *         Page of experiment runs.
         */
        get: operations["list_experiment_runs_api_v1_experiment_runs_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/experiment-runs/{experiment_run_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Experiment Run
         * @description Get an experiment run by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no run has this id.
         *
         *     Args:
         *         experiment_run_id: Id of the run.
         *         service: Experiment run service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored experiment run.
         */
        get: operations["get_experiment_run_api_v1_experiment_runs__experiment_run_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Experiment Run
         * @description Delete an experiment run and its replays.
         *
         *     Clients observe HTTP 204 on success and 404 when no run has this id.
         *
         *     Args:
         *         experiment_run_id: Id of the run.
         *         service: Experiment run service.
         *         actor: Caller context.
         */
        delete: operations["delete_experiment_run_api_v1_experiment_runs__experiment_run_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/experiment-runs/{experiment_run_id}/cancel": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Cancel Experiment Run
         * @description Request cancellation of a running experiment run.
         *
         *     Clients observe HTTP 200 on success, 404 when no run has this id, and
         *     409 when the run is not running.
         *
         *     Args:
         *         experiment_run_id: Id of the run.
         *         actor: Caller context.
         *         cancel: Run cancellation flow, committed across its own transactions.
         *
         *     Returns:
         *         Run carrying the cancel request.
         */
        post: operations["cancel_experiment_run_api_v1_experiment_runs__experiment_run_id__cancel_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/experiment-runs/{experiment_run_id}/jobs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Experiment Run Jobs
         * @description List the jobs backing an experiment run's replays.
         *
         *     Clients observe HTTP 200 on success and 404 when no run has this id.
         *
         *     Args:
         *         experiment_run_id: Id of the run.
         *         service: Experiment run service.
         *         actor: Caller context.
         *         params: Experiment run jobs list params.
         *
         *     Returns:
         *         Page of jobs.
         */
        get: operations["list_experiment_run_jobs_api_v1_experiment_runs__experiment_run_id__jobs_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/experiments": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Experiments
         * @description List experiments.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Experiment service.
         *         actor: Caller context.
         *         params: Experiment list params.
         *
         *     Returns:
         *         Page of experiments.
         */
        get: operations["list_experiments_api_v1_experiments_get"];
        put?: never;
        /**
         * Create Experiment
         * @description Create an experiment.
         *
         *     Clients observe HTTP 201 on success, 404 when the agent does not exist
         *     or an evaluator config names an unknown evaluator or version, 409 when
         *     the name is already registered, and 422 on invalid input, including a
         *     duplicate resolved evaluator version.
         *
         *     Args:
         *         body: Experiment create request.
         *         service: Experiment service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created experiment.
         */
        post: operations["create_experiment_api_v1_experiments_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/experiments/{experiment_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Experiment
         * @description Get an experiment by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no experiment has this
         *     id.
         *
         *     Args:
         *         experiment_id: Id of the experiment.
         *         service: Experiment service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored experiment.
         */
        get: operations["get_experiment_api_v1_experiments__experiment_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Experiment
         * @description Delete an experiment.
         *
         *     Clients observe HTTP 204 on success and 404 when no experiment has this
         *     id.
         *
         *     Args:
         *         experiment_id: Id of the experiment.
         *         service: Experiment service.
         *         actor: Caller context.
         */
        delete: operations["delete_experiment_api_v1_experiments__experiment_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Experiment
         * @description Update an experiment.
         *
         *     Clients observe HTTP 200 on success, 404 when no experiment has this id
         *     or a new evaluator config names an unknown evaluator or version, and 422
         *     on invalid input, including an attempt to clear the name, the tool
         *     policy, or every evaluator.
         *
         *     Args:
         *         experiment_id: Id of the experiment.
         *         body: Experiment update request.
         *         service: Experiment service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated experiment.
         */
        patch: operations["update_experiment_api_v1_experiments__experiment_id__patch"];
        trace?: never;
    };
    "/api/v1/experiments/{experiment_id}/runs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Start Run
         * @description Start an experiment run, fanning out one replay per cohort version session.
         *
         *     Clients observe HTTP 201 on success, 404 when the experiment, the
         *     cohort version, or the resolved agent version does not exist, 409 when
         *     the baseline evaluation mode is not none and a cohort version session
         *     is not finished, and 422 when the cohort version has no sessions, the
         *     cohort version or agent version belongs to another agent, the resolved
         *     agent version has no run spec, or the config carries an override or
         *     tool policy the agent version's runtime capabilities do not declare.
         *
         *     Args:
         *         experiment_id: Id of the experiment.
         *         body: Experiment run create request.
         *         service: Experiment service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created run.
         */
        post: operations["start_run_api_v1_experiments__experiment_id__runs_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/importers": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Importers
         * @description List importers.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Importer service.
         *         actor: Caller context.
         *         params: Importer list params.
         *
         *     Returns:
         *         Page of importers.
         */
        get: operations["list_importers_api_v1_importers_get"];
        put?: never;
        /**
         * Create Importer
         * @description Create an importer.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input.
         *
         *     Args:
         *         body: Importer create request.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created importer.
         */
        post: operations["create_importer_api_v1_importers_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/importers/{importer_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Importer
         * @description Get an importer by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no importer has this
         *     id.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored importer.
         */
        get: operations["get_importer_api_v1_importers__importer_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Importer
         * @description Delete an importer, cascading its versions.
         *
         *     Clients observe HTTP 204 on success, 404 when no importer has this id,
         *     and 409 when one of its versions is referenced by an import.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         service: Importer service.
         *         actor: Caller context.
         */
        delete: operations["delete_importer_api_v1_importers__importer_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Importer
         * @description Update an importer.
         *
         *     Clients observe HTTP 200 on success, 404 when no importer has this id,
         *     and 422 on invalid input.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         body: Importer update request.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated importer.
         */
        patch: operations["update_importer_api_v1_importers__importer_id__patch"];
        trace?: never;
    };
    "/api/v1/importers/{importer_id}/versions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Importer Versions
         * @description List an importer's versions.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         service: Importer service.
         *         actor: Caller context.
         *         params: List params.
         *
         *     Returns:
         *         Page of importer versions.
         */
        get: operations["list_importer_versions_api_v1_importers__importer_id__versions_get"];
        put?: never;
        /**
         * Create Importer Version
         * @description Create an importer version.
         *
         *     Clients observe HTTP 201 on success, 404 when no importer has this id
         *     or a script source names an unknown blob, and 422 on invalid input.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         body: Importer version create request.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created importer version.
         */
        post: operations["create_importer_version_api_v1_importers__importer_id__versions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/importers/{importer_id}/versions/{version}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Importer Version
         * @description Get an importer version by version number.
         *
         *     Clients observe HTTP 200 on success and 404 when no version with this
         *     number exists for this importer.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         version: Version number.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored importer version.
         */
        get: operations["get_importer_version_api_v1_importers__importer_id__versions__version__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update Importer Version
         * @description Update an importer version's display version.
         *
         *     Clients observe HTTP 200 on success and 404 when no version with this
         *     number exists for this importer.
         *
         *     Args:
         *         importer_id: Id of the importer.
         *         version: Version number.
         *         body: Importer version update request.
         *         service: Importer service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated importer version.
         */
        patch: operations["update_importer_version_api_v1_importers__importer_id__versions__version__patch"];
        trace?: never;
    };
    "/api/v1/imports": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Imports
         * @description List imports.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Import service.
         *         actor: Caller context.
         *         params: Import list params.
         *
         *     Returns:
         *         Page of imports.
         */
        get: operations["list_imports_api_v1_imports_get"];
        put?: never;
        /**
         * Create Import
         * @description Import sessions from a payload blob, as a job holding one importer task.
         *
         *     Clients observe HTTP 201 on success, 404 when the importer, the version,
         *     the payload blob, the agent, the agent version, or an evaluator does not
         *     exist, and 422 when the agent version belongs to another agent, an
         *     evaluator is scoped to another agent, or an evaluator version repeats.
         *
         *     Args:
         *         body: Import create request.
         *         service: Import service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created import.
         */
        post: operations["create_import_api_v1_imports_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/imports/{import_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Import
         * @description Get an import by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no import has this id.
         *
         *     Args:
         *         import_id: Id of the import.
         *         service: Import service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored import.
         */
        get: operations["get_import_api_v1_imports__import_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/info": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Info
         * @description Report how this server identifies itself and authenticates its callers.
         *
         *     The endpoint is unauthenticated, because a client has to read it before it
         *     can know which credential to present.
         *
         *     Args:
         *         settings: Service settings governing auth behavior.
         *         server_id: Persisted server id.
         *         ui_version: UI version served by this process.
         *
         *     Returns:
         *         Server info.
         */
        get: operations["get_info_api_v1_info_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/insights": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Insights
         * @description List insights.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Insight service.
         *         actor: Caller context.
         *         params: Insight list params.
         *
         *     Returns:
         *         Page of insights.
         */
        get: operations["list_insights_api_v1_insights_get"];
        put?: never;
        /**
         * Create Insights
         * @description Create a batch of insights for one agent in one shot.
         *
         *     Clients observe HTTP 201 on success, 404 when the agent does not exist,
         *     and 422 on validation.
         *
         *     Args:
         *         body: Insight batch create request.
         *         service: Insight service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created insights in input order.
         */
        post: operations["create_insights_api_v1_insights_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/insights/{insight_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Insight
         * @description Get an insight by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no insight has this id.
         *
         *     Args:
         *         insight_id: Id of the insight.
         *         service: Insight service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored insight.
         */
        get: operations["get_insight_api_v1_insights__insight_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Insight
         * @description Delete an insight.
         *
         *     Clients observe HTTP 204 on success and 404 when no insight has this id.
         *
         *     Args:
         *         insight_id: Id of the insight.
         *         service: Insight service.
         *         actor: Caller context.
         */
        delete: operations["delete_insight_api_v1_insights__insight_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Insight
         * @description Update an insight's title and description.
         *
         *     Clients observe HTTP 200 on success, 404 when no insight has this id,
         *     and 422 when the update clears the insight title.
         *
         *     Args:
         *         insight_id: Id of the insight.
         *         body: Insight update request.
         *         service: Insight service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated insight.
         */
        patch: operations["update_insight_api_v1_insights__insight_id__patch"];
        trace?: never;
    };
    "/api/v1/investigations": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Investigations
         * @description List investigations.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Investigation service.
         *         actor: Caller context.
         *         params: Investigation list params.
         *
         *     Returns:
         *         Page of investigations.
         */
        get: operations["list_investigations_api_v1_investigations_get"];
        put?: never;
        /**
         * Create Investigation
         * @description Create an investigation with its linked sessions in one shot.
         *
         *     Clients observe HTTP 201 on success, 404 when the agent does not exist,
         *     and 422 when a linked session id repeats, is missing, or belongs to a
         *     different agent.
         *
         *     Args:
         *         body: Investigation create request.
         *         service: Investigation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created investigation.
         */
        post: operations["create_investigation_api_v1_investigations_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/investigations/{investigation_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Investigation
         * @description Get an investigation by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no investigation has
         *     this id.
         *
         *     Args:
         *         investigation_id: Id of the investigation.
         *         service: Investigation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored investigation.
         */
        get: operations["get_investigation_api_v1_investigations__investigation_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Investigation
         * @description Delete an investigation, cascading its linked sessions and answers.
         *
         *     Clients observe HTTP 204 on success and 404 when no investigation has
         *     this id.
         *
         *     Args:
         *         investigation_id: Id of the investigation.
         *         service: Investigation service.
         *         actor: Caller context.
         */
        delete: operations["delete_investigation_api_v1_investigations__investigation_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Investigation
         * @description Update an investigation's name, description, and status.
         *
         *     Clients observe HTTP 200 on success, 404 when no investigation has this
         *     id, 409 when the update moves the status backwards, and 422 when the
         *     update clears the investigation name or status.
         *
         *     Args:
         *         investigation_id: Id of the investigation.
         *         body: Investigation update request.
         *         service: Investigation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated investigation.
         */
        patch: operations["update_investigation_api_v1_investigations__investigation_id__patch"];
        trace?: never;
    };
    "/api/v1/investigations/{investigation_id}/sessions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Investigation Sessions
         * @description List an investigation's linked sessions, ordered by position ascending.
         *
         *     Clients observe HTTP 200 on success, 404 when no investigation has this
         *     id, and 422 on invalid pagination parameters.
         *
         *     Args:
         *         investigation_id: Id of the investigation.
         *         service: Investigation service.
         *         actor: Caller context.
         *         params: Investigation sessions list params.
         *
         *     Returns:
         *         Page of investigation sessions.
         */
        get: operations["list_investigation_sessions_api_v1_investigations__investigation_id__sessions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/investigations/{investigation_id}/sessions/{session_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update Investigation Session Verdict
         * @description Set or clear a linked session's verdict.
         *
         *     Clients observe HTTP 200 on success and 404 when no investigation has
         *     this id or no investigation session links this investigation and
         *     session.
         *
         *     Args:
         *         investigation_id: Id of the investigation.
         *         session_id: Id of the linked session.
         *         body: Investigation session update request.
         *         service: Investigation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated investigation session.
         */
        patch: operations["update_investigation_session_verdict_api_v1_investigations__investigation_id__sessions__session_id__patch"];
        trace?: never;
    };
    "/api/v1/jobs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Jobs
         * @description List jobs.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Job service.
         *         actor: Caller context.
         *         params: Job list params.
         *
         *     Returns:
         *         Page of jobs.
         */
        get: operations["list_jobs_api_v1_jobs_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/jobs/{job_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Job
         * @description Get a job by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no job has this id.
         *
         *     Args:
         *         job_id: Id of the job.
         *         service: Job service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored job.
         */
        get: operations["get_job_api_v1_jobs__job_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Job
         * @description Delete a settled job, cascading its tasks.
         *
         *     Clients observe HTTP 204 on success, 404 when no job has this id, and
         *     409 when the job has not settled.
         *
         *     Args:
         *         job_id: Id of the job.
         *         service: Job service.
         *         actor: Caller context.
         */
        delete: operations["delete_job_api_v1_jobs__job_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/jobs/{job_id}/cancel": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Cancel Job
         * @description Request cancellation of a job.
         *
         *     Clients observe HTTP 200 on success, 404 when no job has this id, and
         *     409 when the job already settled.
         *
         *     Args:
         *         job_id: Id of the job.
         *         service: Job service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Job carrying the cancel request.
         */
        post: operations["cancel_job_api_v1_jobs__job_id__cancel_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/jobs/{job_id}/tasks": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Job Tasks
         * @description List the tasks of a job.
         *
         *     Clients observe HTTP 200 on success and 404 when no job has this id.
         *
         *     Args:
         *         job_id: Id of the job.
         *         service: Job service.
         *         actor: Caller context.
         *         params: Job tasks list params.
         *
         *     Returns:
         *         Page of tasks.
         */
        get: operations["list_job_tasks_api_v1_jobs__job_id__tasks_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/login": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Login
         * @description Log in and receive a bearer token.
         *
         *     The ``password`` grant type takes the form username and password and is
         *     accepted under the ``local`` auth scheme. The ``api-key`` grant type reads
         *     an API key from the authorization header and is accepted under the same
         *     scheme. The ``control-plane`` grant type reads a control plane credential
         *     from the authorization header and mirrors the control plane user into a
         *     local account. The device grant type takes the ``device_id`` and
         *     ``device_code`` of a device authorization and returns a token once an
         *     account has confirmed it.
         *
         *     Clients observe HTTP 200 on success, 400 when the grant type is not
         *     accepted by this server or a device authorization is not ready, and 401
         *     when the credentials cannot be validated. A 400 carries an OAuth 2.0
         *     ``error`` code, of which ``authorization_pending`` means the caller
         *     should poll again.
         *
         *     Args:
         *         request: Incoming request.
         *         response: Outgoing response.
         *         settings: Service settings governing auth behavior.
         *         service: Authentication service.
         *         form: Login request form carrying the resolved grant type.
         *
         *     Raises:
         *         HTTPException: The credentials cannot be validated.
         *
         *     Returns:
         *         Issued token.
         */
        post: operations["login_api_v1_login_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/logout": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Logout
         * @description Log out and clear the auth cookie.
         *
         *     Clients observe HTTP 204.
         *
         *     Args:
         *         response: Outgoing response.
         *         settings: Service settings governing auth behavior.
         */
        post: operations["logout_api_v1_logout_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/replays": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Replays
         * @description List replays.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Replay service.
         *         actor: Caller context.
         *         params: Replay list params.
         *
         *     Returns:
         *         Page of replays.
         */
        get: operations["list_replays_api_v1_replays_get"];
        put?: never;
        /**
         * Create Replay
         * @description Create a standalone replay of a recorded or imported session.
         *
         *     Clients observe HTTP 201 on success, 404 when the baseline session or
         *     the resolved agent version or an evaluator config does not exist, 409
         *     when the baseline evaluation mode is not none and the baseline session
         *     is not finished, and 422 when the baseline session carries no agent
         *     version and none was given, the resolved agent version has no run spec,
         *     the tool policy uses cohort-version-scoped history, the config carries
         *     an override or tool policy the agent version's runtime capabilities do
         *     not declare, or an evaluator version repeats.
         *
         *     Args:
         *         body: Replay create request.
         *         service: Replay service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created replay.
         */
        post: operations["create_replay_api_v1_replays_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/replays/{replay_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Replay
         * @description Get a replay by id.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token names a task
         *     outside this replay's job, and 404 when no replay has this id.
         *
         *     Args:
         *         replay_id: Id of the replay.
         *         service: Replay service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored replay.
         */
        get: operations["get_replay_api_v1_replays__replay_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Replay
         * @description Delete a replay.
         *
         *     Clients observe HTTP 204 on success, 404 when no replay has this id, and
         *     409 when the replay belongs to an experiment run.
         *
         *     Args:
         *         replay_id: Id of the replay.
         *         service: Replay service.
         *         actor: Caller context.
         */
        delete: operations["delete_replay_api_v1_replays__replay_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/replays/{replay_id}/tool-lookup": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Tool Lookup
         * @description Search recorded tool-call history for a cached result.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token names a task
         *     outside this replay's job, 404 when no replay has this id, and 422 when
         *     the tool is not configured for history or an occurrence was given for a
         *     non-baseline history scope.
         *
         *     Args:
         *         replay_id: Id of the replay.
         *         body: Tool lookup request.
         *         service: Replay service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Matching recorded tool call, unset on a miss.
         */
        post: operations["tool_lookup_api_v1_replays__replay_id__tool_lookup_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/secrets": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Secrets
         * @description List secrets.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters. List responses never include secret values.
         *
         *     Args:
         *         service: Secret service.
         *         actor: Caller context.
         *         params: Secret list params.
         *
         *     Returns:
         *         Page of secrets without values.
         */
        get: operations["list_secrets_api_v1_secrets_get"];
        put?: never;
        /**
         * Create Secret
         * @description Create a secret.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input. The response omits the secret
         *     values.
         *
         *     Args:
         *         body: Secret create request.
         *         service: Secret service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created secret without values.
         */
        post: operations["create_secret_api_v1_secrets_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/secrets/{secret_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Secret
         * @description Get a secret by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no secret has this
         *     id.
         *
         *     Args:
         *         secret_id: Id of the secret.
         *         service: Secret service.
         *         actor: Caller context.
         *         include_values: Whether to include the secret values.
         *
         *     Returns:
         *         Stored secret.
         */
        get: operations["get_secret_api_v1_secrets__secret_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Secret
         * @description Delete a secret.
         *
         *     Clients observe HTTP 204 on success, 404 when no secret has this id,
         *     and 409 when an agent version references it.
         *
         *     Args:
         *         secret_id: Id of the secret.
         *         service: Secret service.
         *         actor: Caller context.
         */
        delete: operations["delete_secret_api_v1_secrets__secret_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Secret
         * @description Update a secret.
         *
         *     Clients observe HTTP 200 on success, 404 when no secret has this id,
         *     and 422 on invalid input.
         *
         *     Args:
         *         secret_id: Id of the secret.
         *         body: Secret update request.
         *         service: Secret service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated secret without values.
         */
        patch: operations["update_secret_api_v1_secrets__secret_id__patch"];
        trace?: never;
    };
    "/api/v1/service-accounts": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create Service Account
         * @description Create a service account, active without credentials.
         *
         *     Clients observe HTTP 201 on success, 403 outside the ``local`` auth
         *     scheme and when the caller may not create accounts, 409 when the name is
         *     already registered, and 422 on invalid input.
         *
         *     Args:
         *         body: Service account create request.
         *         service: Account service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created account.
         */
        post: operations["create_service_account_api_v1_service_accounts_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/service-accounts/{account_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update Service Account
         * @description Partially update a service account.
         *
         *     Clients observe HTTP 200 on success, 403 outside the ``local`` auth
         *     scheme and when the caller may not update service accounts, 404 when the
         *     service account does not exist, and 422 on invalid input.
         *
         *     Args:
         *         account_id: Id of the account.
         *         body: Service account update request.
         *         service: Account service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated account.
         */
        patch: operations["update_service_account_api_v1_service_accounts__account_id__patch"];
        trace?: never;
    };
    "/api/v1/session-runs": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create Session Run
         * @description Run an agent version once, as a job holding one agent task.
         *
         *     Clients observe HTTP 201 on success, 404 when no agent version has this
         *     id, and 422 when the agent version carries no run spec.
         *
         *     Args:
         *         body: Session run create request.
         *         service: Job service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created job.
         */
        post: operations["create_session_run_api_v1_session_runs_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sessions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Sessions
         * @description List sessions.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Session service.
         *         actor: Caller context.
         *         params: Session list params.
         *
         *     Returns:
         *         Page of sessions, with payloads when include_payloads is set.
         */
        get: operations["list_sessions_api_v1_sessions_get"];
        put?: never;
        /**
         * Create Session
         * @description Create a session.
         *
         *     A task principal's session is always linked to its own task, regardless
         *     of the request's task_id. Clients observe HTTP 201 on success, 409 when
         *     the imported_from and external id pair is already registered, and 422 on
         *     invalid input.
         *
         *     Args:
         *         body: Session create request.
         *         service: Session service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created session.
         */
        post: operations["create_session_api_v1_sessions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sessions/{session_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Session
         * @description Get a session by id.
         *
         *     Clients observe HTTP 200 on success and 404 when no session has this
         *     id.
         *
         *     Args:
         *         session_id: Id of the session.
         *         service: Session service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored session.
         */
        get: operations["get_session_api_v1_sessions__session_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Session
         * @description Delete a session.
         *
         *     Deleting a session cascades its nodes. Clients observe HTTP 204 on
         *     success, 404 when no session has this id, and 409 when the session is
         *     referenced by a cohort version, investigation, or replay.
         *
         *     Args:
         *         session_id: Id of the session.
         *         service: Session service.
         *         actor: Caller context.
         */
        delete: operations["delete_session_api_v1_sessions__session_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Session
         * @description Update a session.
         *
         *     Clients observe HTTP 200 on success, 404 when no session has this id,
         *     409 when the session is no longer in progress, and 422 on invalid
         *     input, including an attempt to clear the status.
         *
         *     Args:
         *         session_id: Id of the session.
         *         body: Session update request.
         *         service: Session service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated session.
         */
        patch: operations["update_session_api_v1_sessions__session_id__patch"];
        trace?: never;
    };
    "/api/v1/sessions/{session_id}/evaluations": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create Session Evaluations
         * @description Create manual evaluations on a session.
         *
         *     Clients observe HTTP 200 on success, 404 when no session has this id,
         *     409 when the session is not finished or an evaluation name already
         *     exists for the session, and 422 when the request names the same
         *     evaluation twice.
         *
         *     Args:
         *         session_id: Id of the session to create evaluations on.
         *         body: Session evaluations request.
         *         service: Evaluation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored evaluations in request order.
         */
        post: operations["create_session_evaluations_api_v1_sessions__session_id__evaluations_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sessions/{session_id}/full": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Session With Nodes
         * @description Get a session together with every one of its nodes.
         *
         *     The node list is not paginated, so one call carries a whole session.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token neither owns
         *     nor reads this session, and 404 when no session has this id.
         *
         *     Args:
         *         session_id: Id of the session.
         *         service: Session service.
         *         node_service: Session node service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Session with every node, ordered by index.
         */
        get: operations["get_session_with_nodes_api_v1_sessions__session_id__full_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/sessions/{session_id}/nodes": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Session Nodes
         * @description List the nodes of a session, ordered by index ascending.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token neither owns
         *     nor reads this session, and 422 on invalid pagination parameters.
         *
         *     Args:
         *         session_id: Id of the session.
         *         service: Session node service.
         *         actor: Caller context.
         *         params: Session node list params.
         *
         *     Returns:
         *         Page of session nodes, ordered by index.
         */
        get: operations["list_session_nodes_api_v1_sessions__session_id__nodes_get"];
        put?: never;
        /**
         * Ingest Session Nodes
         * @description Ingest a batch of session nodes.
         *
         *     An index already stored is replaced whole, matching the upsert
         *     semantics of ``POST /api/v1/workers``. Clients observe HTTP 200 on success,
         *     404 when no session has this id, 409 when the session does not
         *     currently accept node ingestion, and 422 when a parent_index does not
         *     resolve.
         *
         *     Args:
         *         session_id: Id of the session to ingest into.
         *         body: Session node batch request.
         *         service: Session node service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored nodes in batch order, with reasoning, inputs, outputs, and
         *         attributes null.
         */
        post: operations["ingest_session_nodes_api_v1_sessions__session_id__nodes_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tags": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Tags
         * @description List tags.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Tag service.
         *         actor: Caller context.
         *         params: Tag list params.
         *
         *     Returns:
         *         Page of tags.
         */
        get: operations["list_tags_api_v1_tags_get"];
        put?: never;
        /**
         * Create Tag
         * @description Create a tag.
         *
         *     Clients observe HTTP 201 on success, 409 when the name is already
         *     registered, and 422 on invalid input.
         *
         *     Args:
         *         body: Tag create request.
         *         service: Tag service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created tag.
         */
        post: operations["create_tag_api_v1_tags_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tags/{tag_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete Tag
         * @description Delete a tag.
         *
         *     Deleting a tag cascades its links. Clients observe HTTP 204 on success
         *     and 404 when no tag has this id.
         *
         *     Args:
         *         tag_id: Id of the tag.
         *         service: Tag service.
         *         actor: Caller context.
         */
        delete: operations["delete_tag_api_v1_tags__tag_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Tag
         * @description Update a tag.
         *
         *     Clients observe HTTP 200 on success, 404 when no tag has this id, 409
         *     when the new name is already registered, and 422 on invalid input.
         *
         *     Args:
         *         tag_id: Id of the tag.
         *         body: Tag update request.
         *         service: Tag service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Updated tag.
         */
        patch: operations["update_tag_api_v1_tags__tag_id__patch"];
        trace?: never;
    };
    "/api/v1/tags/{tag_id}/links": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create Tag Link
         * @description Link a tag to a resource.
         *
         *     Clients observe HTTP 201 on success, 404 when no tag or no resource of
         *     the given type and id exists, 409 when the link is already registered,
         *     and 422 on invalid input.
         *
         *     Args:
         *         tag_id: Id of the tag.
         *         body: Tag link create request.
         *         service: Tag service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created tag link.
         */
        post: operations["create_tag_link_api_v1_tags__tag_id__links_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tags/{tag_id}/links/{resource_type}/{resource_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Delete Tag Link
         * @description Unlink a tag from a resource.
         *
         *     Clients observe HTTP 204 on success and 404 when no link matches the
         *     tag and resource.
         *
         *     Args:
         *         tag_id: Id of the tag.
         *         resource_type: Kind of the linked resource.
         *         resource_id: Id of the linked resource.
         *         service: Tag service.
         *         actor: Caller context.
         */
        delete: operations["delete_tag_link_api_v1_tags__tag_id__links__resource_type___resource_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tasks": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Tasks
         * @description List tasks.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Task service.
         *         actor: Caller context.
         *         params: Task list params.
         *
         *     Returns:
         *         Page of tasks.
         */
        get: operations["list_tasks_api_v1_tasks_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tasks/claim": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Claim Tasks
         * @description Claim pending tasks matching the worker's stored scope.
         *
         *     Clients observe HTTP 200 on success, 403 when the caller holds no worker
         *     token, 404 when no worker has the claiming principal's id, and 422 on
         *     invalid input.
         *
         *     Args:
         *         body: Task claim request.
         *         service: Task service.
         *         auth_service: Authentication service for the current request.
         *         actor: Caller context.
         *
         *     Returns:
         *         Claimed tasks with their execution specs and a task token each.
         */
        post: operations["claim_tasks_api_v1_tasks_claim_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/tasks/{task_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Task
         * @description Get a task by id.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token names a
         *     different task, and 404 when no task has this id.
         *
         *     Args:
         *         task_id: Id of the task.
         *         service: Task service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored task.
         */
        get: operations["get_task_api_v1_tasks__task_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update Task
         * @description Apply an executor transition to a task.
         *
         *     Clients observe HTTP 200 on success, 403 when the caller holds no task
         *     token for this task, 404 when no task has this id, 409 when the attempt
         *     does not match or the transition is illegal, 413 when the result exceeds
         *     the size cap, and 422 when the body carries no status.
         *
         *     Args:
         *         task_id: Id of the task.
         *         body: Task update request.
         *         service: Task service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Task carrying its new status.
         */
        patch: operations["update_task_api_v1_tasks__task_id__patch"];
        trace?: never;
    };
    "/api/v1/tasks/{task_id}/spec": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Task Spec
         * @description Get the execution spec of a task.
         *
         *     Clients observe HTTP 200 on success, 403 when a task token names a
         *     different task, and 404 when no task has this id or the spec references
         *     a missing resource.
         *
         *     Args:
         *         task_id: Id of the task.
         *         service: Task service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Execution spec.
         */
        get: operations["get_task_spec_api_v1_tasks__task_id__spec_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/ui/experiment-runs/{experiment_run_id}/evaluation-aggregates": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Experiment Run Evaluation Aggregates
         * @description Aggregate the evaluations linked to an experiment run's replays.
         *
         *     The input set is the evaluations linked to the run's replays, grouped by
         *     name, evaluator version, and data type. Baseline and result sessions are
         *     aggregated separately, and each aggregate carries the per-replay
         *     evaluation values of the 50 most recent replays. Clients observe HTTP
         *     200 on success and 404 when no experiment run has this id.
         *
         *     Args:
         *         experiment_run_id: Id of the experiment run.
         *         run_service: Experiment run service.
         *         replay_service: Replay service.
         *         evaluation_service: Evaluation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         One aggregate per evaluation name, evaluator version, and data
         *         type, sorted by name.
         */
        get: operations["list_experiment_run_evaluation_aggregates_api_v1_ui_experiment_runs__experiment_run_id__evaluation_aggregates_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/ui/sample-data": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create Sample Data
         * @description Seed the sample agent and everything recorded under it.
         *
         *     Clients observe HTTP 201 on success and 409 when the agent name is
         *     already registered.
         *
         *     Args:
         *         seeder: Sample data seeder.
         *         session: Request-scoped database session.
         *         actor: Caller context.
         *         body: Sample data create request, None uses the sample data's agent name.
         *
         *     Returns:
         *         Agent the sample data was seeded under.
         */
        post: operations["create_sample_data_api_v1_ui_sample_data_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/ui/sessions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Sessions With Evaluations
         * @description List sessions, each with every evaluation of the session.
         *
         *     Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         session_service: Session service.
         *         evaluation_service: Evaluation service.
         *         actor: Caller context.
         *         params: Session list params.
         *
         *     Returns:
         *         Page of sessions with their evaluations.
         */
        get: operations["list_sessions_with_evaluations_api_v1_ui_sessions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/ui/sessions/{session_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Session With Evaluations
         * @description Get a session by id, with every evaluation of the session.
         *
         *     Clients observe HTTP 200 on success and 404 when no session has this
         *     id.
         *
         *     Args:
         *         session_id: Id of the session.
         *         session_service: Session service.
         *         evaluation_service: Evaluation service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Stored session with its evaluations.
         */
        get: operations["get_session_with_evaluations_api_v1_ui_sessions__session_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/users": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Create User
         * @description Create a user.
         *
         *     A user created without a password starts inactive and its response
         *     carries the activation token once.
         *
         *     Clients observe HTTP 201 on success, 403 outside the ``local`` auth
         *     scheme, 409 when the name is already registered, and 422 on invalid input.
         *
         *     Args:
         *         body: User create request.
         *         service: Account service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Created account.
         */
        post: operations["create_user_api_v1_users_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/users/{account_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        /**
         * Update User
         * @description Partially update a user.
         *
         *     A password write carries the current password in ``old_password``.
         *
         *     Clients observe HTTP 200 on success, 403 when writing a password or the
         *     admin flag outside the ``local`` auth scheme, when writing another
         *     account's password or metadata, when changing the calling account's own
         *     admin flag, and when the supplied current password is missing or wrong,
         *     404 when the user does not exist, and 422 on invalid input.
         *
         *     Args:
         *         account_id: Id of the account.
         *         body: User update request.
         *         service: Account service.
         *         actor: Caller context.
         *         settings: API settings for this process.
         *
         *     Returns:
         *         Updated account.
         */
        patch: operations["update_user_api_v1_users__account_id__patch"];
        trace?: never;
    };
    "/api/v1/users/{account_id}/activate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Activate User
         * @description Activate a user with its activation token and a new password.
         *
         *     The route is unauthenticated, because the account it activates cannot log
         *     in until it holds a password.
         *
         *     Clients observe HTTP 200 on success, 403 when the account has no pending
         *     token or the token does not match, 404 when the user does not exist, and
         *     422 on invalid input.
         *
         *     Args:
         *         account_id: Id of the account.
         *         body: User activate request.
         *         service: Account service.
         *
         *     Returns:
         *         Activated account.
         */
        post: operations["activate_user_api_v1_users__account_id__activate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/users/{account_id}/deactivate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Deactivate User
         * @description Deactivate a user and return its fresh activation token once.
         *
         *     Clients observe HTTP 200 on success, 403 outside the ``local`` auth scheme
         *     and when deactivating the calling account, and 404 when the user does not
         *     exist.
         *
         *     Args:
         *         account_id: Id of the account.
         *         service: Account service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Deactivated account carrying its activation token.
         */
        post: operations["deactivate_user_api_v1_users__account_id__deactivate_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workers": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Workers
         * @description List workers.
         *
         *     Workers past the liveness window are left out unless include_stale is
         *     set. Clients observe HTTP 200 on success and 422 on invalid pagination
         *     parameters.
         *
         *     Args:
         *         service: Worker service.
         *         actor: Caller context.
         *         settings: API settings for this process.
         *         params: Worker list params.
         *
         *     Returns:
         *         Page of workers.
         */
        get: operations["list_workers_api_v1_workers_get"];
        put?: never;
        /**
         * Register Worker
         * @description Register a worker.
         *
         *     Every registration creates a new worker, names are labels and need not
         *     be unique. Clients observe HTTP 200 on success, 426 from an SDK that
         *     renews by re-registering, and 422 on invalid input.
         *
         *     Args:
         *         body: Worker create request.
         *         service: Worker service.
         *         auth_service: Authentication service for the current request.
         *         actor: Caller context.
         *         settings: API settings for this process.
         *         client: Client identification header value.
         *
         *     Returns:
         *         Stored worker with a bearer token scoped to it.
         */
        post: operations["register_worker_api_v1_workers_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workers/{worker_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Worker
         * @description Get a worker by id.
         *
         *     Clients observe HTTP 200 on success, 403 when a worker token names a
         *     different worker, and 404 when no worker has this id.
         *
         *     Args:
         *         worker_id: Id of the worker.
         *         service: Worker service.
         *         actor: Caller context.
         *         settings: API settings for this process.
         *
         *     Returns:
         *         Stored worker.
         */
        get: operations["get_worker_api_v1_workers__worker_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Worker
         * @description Delete a worker.
         *
         *     Clients observe HTTP 204 on success and 404 when no worker has this id.
         *
         *     Args:
         *         worker_id: Id of the worker.
         *         service: Worker service.
         *         actor: Caller context.
         */
        delete: operations["delete_worker_api_v1_workers__worker_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workers/{worker_id}/heartbeat": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Heartbeat Worker
         * @description Report the tasks a worker currently holds.
         *
         *     Clients observe HTTP 200 on success, 403 when the caller holds no worker
         *     token for this worker, and 404 when no worker has this id.
         *
         *     Args:
         *         worker_id: Id of the worker.
         *         body: Worker heartbeat request.
         *         service: Task service.
         *         actor: Caller context.
         *
         *     Returns:
         *         Held tasks the worker should stop running.
         */
        post: operations["heartbeat_worker_api_v1_workers__worker_id__heartbeat_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/workers/{worker_id}/token": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Renew Worker Token
         * @description Issue a fresh token for a registered worker.
         *
         *     Renewal stamps last_seen_at. Clients observe HTTP 200 on success, 403
         *     when the worker belongs to another account, and 404 when no worker has
         *     this id.
         *
         *     Args:
         *         worker_id: Id of the worker.
         *         service: Worker service.
         *         auth_service: Authentication service for the current request.
         *         actor: Caller context.
         *
         *     Returns:
         *         Bearer token scoped to the worker.
         */
        post: operations["renew_worker_token_api_v1_workers__worker_id__token_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/health": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Health
         * @description Report service and database readiness.
         *
         *     Args:
         *         db: Database session for the readiness probe.
         *
         *     Raises:
         *         HTTPException: HTTP 503 when the database probe fails.
         *
         *     Returns:
         *         JSON object with ``status`` and ``database`` fields.
         */
        get: operations["health_health_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/health/live": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Liveness
         * @description Confirm the API process is running.
         *
         *     Returns:
         *         Minimal liveness JSON body.
         */
        get: operations["liveness_health_live_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        /**
         * AccountResponse
         * @description Account response.
         */
        AccountResponse: {
            /**
             * Active
             * @description Whether the account can authenticate.
             */
            active: boolean;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Email
             * @description Contact email.
             */
            email: string | null;
            /**
             * Id
             * Format: uuid
             * @description Account id.
             */
            id: string;
            /**
             * Is Admin
             * @description Whether the account has admin rights.
             */
            is_admin: boolean;
            /**
             * Is Service Account
             * @description Whether this is a service account.
             */
            is_service_account: boolean;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Account name.
             */
            name: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * AgentCapabilities
         * @description Agent capabilities.
         */
        AgentCapabilities: {
            /**
             * Mcp Servers
             * @description MCP servers the agent connects to.
             */
            mcp_servers?: string[];
            /**
             * Skills
             * @description Skills the agent exposes.
             */
            skills?: string[];
            /**
             * Tools
             * @description Tools the agent exposes.
             */
            tools?: string[];
        };
        /**
         * AgentCreateRequest
         * @description Agent create request.
         */
        AgentCreateRequest: {
            /**
             * Description
             * @description Agent description.
             */
            description?: string | null;
            /**
             * Name
             * @description Agent name.
             */
            name: string;
        };
        /**
         * AgentResponse
         * @description Agent response.
         */
        AgentResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Agent description.
             */
            description: string | null;
            /**
             * Id
             * Format: uuid
             * @description Agent id.
             */
            id: string;
            /**
             * Latest Version
             * @description Highest version number created for this agent.
             */
            latest_version: number;
            /**
             * Name
             * @description Agent name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * AgentTaskDetails
         * @description Agent task details.
         */
        AgentTaskDetails: {
            /**
             * Inputs
             * @description Inputs passed to the agent's command.
             */
            inputs: unknown;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            kind: "agent";
            /**
             * Replay Id
             * @description Replay the task runs for.
             */
            replay_id?: string | null;
        };
        /**
         * AgentUpdateRequest
         * @description Agent update request.
         */
        AgentUpdateRequest: {
            /**
             * Description
             * @description New agent description.
             */
            description?: string | null;
            /**
             * Name
             * @description New agent name.
             */
            name?: string | null;
        };
        /**
         * AgentVersionCreateRequest
         * @description Agent version create request.
         */
        AgentVersionCreateRequest: {
            /** @description Agent capabilities. */
            capabilities?: components["schemas"]["AgentCapabilities"] | null;
            /**
             * Description
             * @description Version description.
             */
            description?: string | null;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version?: string | null;
            /** @description Run spec. */
            run_spec?: components["schemas"]["RunSpec"] | null;
        };
        /**
         * AgentVersionResponse
         * @description Agent version response.
         */
        AgentVersionResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent this version belongs to.
             */
            agent_id: string;
            capabilities: components["schemas"]["AgentCapabilities"];
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Version description.
             */
            description: string | null;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version: string | null;
            /**
             * Id
             * Format: uuid
             * @description Agent version id.
             */
            id: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /** @description Run spec. */
            run_spec: components["schemas"]["RunSpec"] | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Version
             * @description Server-assigned version number.
             */
            version: number;
        };
        /**
         * AgentVersionUpdateRequest
         * @description Agent version update request.
         */
        AgentVersionUpdateRequest: {
            /** @description New agent capabilities. */
            capabilities?: components["schemas"]["AgentCapabilities"] | null;
            /**
             * Description
             * @description New version description.
             */
            description?: string | null;
            /**
             * Display Version
             * @description New human-readable designator.
             */
            display_version?: string | null;
            /** @description New run spec. */
            run_spec?: components["schemas"]["RunSpec"] | null;
        };
        /**
         * AndFilter
         * @description And filter.
         */
        AndFilter: {
            /**
             * And
             * @description Operands.
             */
            and: (components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"])[];
        };
        /**
         * AnnotationResponse
         * @description Annotation response.
         */
        AnnotationResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Annotation id.
             */
            id: string;
            /**
             * Investigation Session Id
             * @description Investigation session being answered.
             */
            investigation_session_id?: string | null;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Question Key
             * @description Key of the question being answered.
             */
            question_key?: string | null;
            /** @description Part of the session being annotated. */
            selector?: components["schemas"]["AnnotationSelector"] | null;
            /**
             * Session Id
             * Format: uuid
             * @description Session being annotated.
             */
            session_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Value
             * @description Annotation value.
             */
            value: unknown;
        };
        /**
         * AnnotationSelector
         * @description Annotation selector.
         */
        AnnotationSelector: {
            /**
             * Node Id
             * @description Targeted node.
             */
            node_id?: string | null;
            /**
             * Path
             * @description RFC 6901 JSON Pointer into the targeted node or the session response.
             */
            path?: string | null;
            /** @description Character range within the resolved string. */
            span?: components["schemas"]["AnnotationSpan"] | null;
        };
        /**
         * AnnotationSpan
         * @description Annotation span.
         */
        AnnotationSpan: {
            /**
             * End
             * @description End offset of the character range.
             */
            end: number;
            /**
             * Start
             * @description Start offset of the character range.
             */
            start: number;
        };
        /**
         * AnnotationUpdateRequest
         * @description Annotation update request.
         */
        AnnotationUpdateRequest: {
            /**
             * Value
             * @description New annotation value.
             */
            value: unknown;
        };
        /**
         * ApiKeyCreateRequest
         * @description API key create request.
         */
        ApiKeyCreateRequest: {
            /**
             * Name
             * @description API key name.
             */
            name: string;
        };
        /**
         * ApiKeyIssuedResponse
         * @description API key response carrying newly issued key material.
         */
        ApiKeyIssuedResponse: {
            /**
             * Active
             * @description Whether the key can authenticate.
             */
            active: boolean;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description API key id.
             */
            id: string;
            /**
             * Key
             * @description Plaintext key, shown once.
             */
            key: string;
            /**
             * Last Rotated
             * @description Time of the last rotation.
             */
            last_rotated: string | null;
            /**
             * Last Used
             * @description Time of the last use for authentication.
             */
            last_used: string | null;
            /**
             * Name
             * @description API key name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ApiKeyResponse
         * @description API key response.
         */
        ApiKeyResponse: {
            /**
             * Active
             * @description Whether the key can authenticate.
             */
            active: boolean;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description API key id.
             */
            id: string;
            /**
             * Last Rotated
             * @description Time of the last rotation.
             */
            last_rotated: string | null;
            /**
             * Last Used
             * @description Time of the last use for authentication.
             */
            last_used: string | null;
            /**
             * Name
             * @description API key name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ApiKeyRotateRequest
         * @description API key rotate request.
         */
        ApiKeyRotateRequest: {
            /**
             * Retain Period Minutes
             * @description Number of minutes the previous key remains valid after rotation.
             * @default 0
             */
            retain_period_minutes: number;
        };
        /**
         * ApiKeyUpdateRequest
         * @description API key update request.
         */
        ApiKeyUpdateRequest: {
            /**
             * Active
             * @description New active state.
             */
            active: boolean;
        };
        /**
         * AuthScheme
         * @description Authentication scheme.
         * @enum {string}
         */
        AuthScheme: "none" | "local" | "control_plane";
        /**
         * BaselineEvaluationMode
         * @description Baseline evaluation mode.
         * @enum {string}
         */
        BaselineEvaluationMode: "none" | "if_missing" | "force";
        /**
         * Bin
         * @description Bin.
         */
        Bin: {
            /**
             * Count
             * @description Observations in the bin.
             */
            count: number;
            /**
             * Lower Bound
             * @description Inclusive lower bound, None on an open-ended first bin.
             */
            lower_bound?: number | null;
            /**
             * Upper Bound
             * @description Exclusive upper bound, None on an open-ended last bin.
             */
            upper_bound?: number | null;
        };
        /**
         * BinnedInsightData
         * @description Binned insight data.
         */
        BinnedInsightData: {
            /**
             * Bins
             * @description Bins, in ascending order.
             */
            bins: components["schemas"]["Bin"][];
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "binned";
            /**
             * Unit
             * @description Unit of the values.
             */
            unit?: string | null;
        };
        /**
         * BlobResponse
         * @description Blob response.
         */
        BlobResponse: {
            /**
             * Created
             * Format: date-time
             * @description Upload time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Blob id.
             */
            id: string;
            /**
             * Media Type
             * @description Content media type.
             */
            media_type: string;
            /**
             * Sha256
             * @description Content hash.
             */
            sha256: string;
            /**
             * Size
             * @description Content size in bytes.
             */
            size: number;
        };
        /** Body_device_authorization_api_v1_device_authorization_post */
        Body_device_authorization_api_v1_device_authorization_post: {
            /** Client Version */
            client_version?: string | null;
            /** Hostname */
            hostname?: string | null;
            /** Os */
            os?: string | null;
            /** Python Version */
            python_version?: string | null;
        };
        /** Body_login_api_v1_login_post */
        Body_login_api_v1_login_post: {
            /** Device Code */
            device_code?: string | null;
            /** Device Id */
            device_id?: string | null;
            /** Grant Type */
            grant_type?: string | null;
            /** Password */
            password?: string | null;
            /** Username */
            username?: string | null;
        };
        /** Body_upload_blob_api_v1_blobs_post */
        Body_upload_blob_api_v1_blobs_post: {
            /** File */
            file: string;
        };
        /**
         * CategoricalInsightData
         * @description Categorical insight data.
         */
        CategoricalInsightData: {
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "categorical";
            /**
             * Unit
             * @description Unit of the values.
             */
            unit?: string | null;
            /**
             * Values
             * @description Values per category.
             */
            values: components["schemas"]["CategoryValue"][];
        };
        /**
         * CategoryValue
         * @description Category value.
         */
        CategoryValue: {
            /**
             * Label
             * @description Category label.
             */
            label: string;
            /**
             * Value
             * @description Measured value.
             */
            value: number;
        };
        /**
         * CohortCreateRequest
         * @description Cohort create request.
         */
        CohortCreateRequest: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the cohort's sessions belong to.
             */
            agent_id: string;
            /**
             * Description
             * @description Cohort description.
             */
            description?: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Cohort name.
             */
            name: string;
        };
        /**
         * CohortResponse
         * @description Cohort response.
         */
        CohortResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the cohort's sessions belong to.
             */
            agent_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Cohort description.
             */
            description: string | null;
            /**
             * Id
             * Format: uuid
             * @description Cohort id.
             */
            id: string;
            /**
             * Latest Version
             * @description Highest version number created for this cohort.
             */
            latest_version: number;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Cohort name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * CohortUpdateRequest
         * @description Cohort update request.
         */
        CohortUpdateRequest: {
            /**
             * Description
             * @description New cohort description.
             */
            description?: string | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
            /**
             * Name
             * @description New cohort name.
             */
            name?: string | null;
        };
        /**
         * CohortVersionCreateRequest
         * @description Cohort version create request.
         */
        CohortVersionCreateRequest: {
            /**
             * Add Session Ids
             * @description Sessions to add to the new version.
             */
            add_session_ids?: string[];
            /**
             * Baseline Id
             * @description Version the delta applies to, None uses the latest version.
             */
            baseline_id?: string | null;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version?: string | null;
            /**
             * Remove Session Ids
             * @description Sessions to remove from the new version.
             */
            remove_session_ids?: string[];
        };
        /**
         * CohortVersionResponse
         * @description Cohort version response.
         */
        CohortVersionResponse: {
            /**
             * Cohort Id
             * Format: uuid
             * @description Cohort this version belongs to.
             */
            cohort_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version: string | null;
            /**
             * Id
             * Format: uuid
             * @description Cohort version id.
             */
            id: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Session Count
             * @description Number of sessions in the version.
             */
            session_count: number;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Version
             * @description Server-assigned version number.
             */
            version: number;
        };
        /**
         * CohortVersionUpdateRequest
         * @description Cohort version update request.
         */
        CohortVersionUpdateRequest: {
            /**
             * Display Version
             * @description New human-readable designator.
             */
            display_version?: string | null;
        };
        /**
         * CopyWorkdirHook
         * @description Copy workdir hook.
         */
        CopyWorkdirHook: {
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "copy_workdir";
        };
        /**
         * DeviceAuthorizationResponse
         * @description Device authorization response.
         */
        DeviceAuthorizationResponse: {
            /**
             * Device Code
             * @description Code the device presents when polling.
             */
            device_code: string;
            /**
             * Device Id
             * Format: uuid
             * @description Device id to verify.
             */
            device_id: string;
            /**
             * Expires In
             * @description Code lifetime in seconds.
             */
            expires_in: number;
            /**
             * Interval
             * @description Seconds to wait between polls.
             */
            interval: number;
            /**
             * User Code
             * @description Code the user confirms in the browser.
             */
            user_code: string;
            /**
             * Verification Uri
             * @description Page where the user enters the code.
             */
            verification_uri: string;
            /**
             * Verification Uri Complete
             * @description Verification page with the code already filled in.
             */
            verification_uri_complete: string;
        };
        /**
         * DeviceResponse
         * @description Device response.
         */
        DeviceResponse: {
            /**
             * Client Version
             * @description Kitaru version the device reported.
             */
            client_version: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Expires
             * @description Expiry time, null for a device that never expires.
             */
            expires: string | null;
            /**
             * Hostname
             * @description Host the device reported.
             */
            hostname: string | null;
            /**
             * Id
             * Format: uuid
             * @description Device id.
             */
            id: string;
            /**
             * Ip Address
             * @description Address the authorization came from.
             */
            ip_address: string | null;
            /**
             * Last Login
             * @description Time of the last authentication with this device.
             */
            last_login: string | null;
            /**
             * Locked
             * @description Whether the device can authenticate.
             */
            locked: boolean;
            /**
             * Os
             * @description Operating system the device reported.
             */
            os: string | null;
            /**
             * Python Version
             * @description Python version the device reported.
             */
            python_version: string | null;
            /** @description Device status. */
            status: components["schemas"]["DeviceStatus"];
            /**
             * Trusted
             * @description Whether the device has the trusted lifetime.
             */
            trusted: boolean;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * DeviceStatus
         * @description Authorized device status.
         * @enum {string}
         */
        DeviceStatus: "pending" | "verified" | "active";
        /**
         * DeviceUpdateRequest
         * @description Device update request.
         */
        DeviceUpdateRequest: {
            /**
             * Locked
             * @description New locked state, left unchanged when omitted.
             */
            locked?: boolean | null;
            /**
             * Trusted
             * @description New trusted state, left unchanged when omitted.
             */
            trusted?: boolean | null;
        };
        /**
         * DeviceVerifyRequest
         * @description Device verify request.
         */
        DeviceVerifyRequest: {
            /**
             * Trusted
             * @description Whether to grant the trusted device lifetime.
             * @default false
             */
            trusted: boolean;
            /**
             * User Code
             * @description User code shown on the device.
             */
            user_code: string;
        };
        /**
         * ErrorBody
         * @description Error body.
         */
        ErrorBody: {
            /**
             * Detail
             * @description Error message.
             */
            detail: string;
        };
        /**
         * EvaluationAggregateResponse
         * @description Evaluation aggregate response.
         */
        EvaluationAggregateResponse: {
            /** @description Stats over the baseline sessions. */
            baseline: components["schemas"]["EvaluationStats"];
            /** @description Evaluation data type. */
            data_type: components["schemas"]["EvaluationDataType"];
            /**
             * Evaluator Name
             * @description Name of the evaluator that produced the group.
             */
            evaluator_name?: string | null;
            /**
             * Evaluator Version
             * @description Version of the evaluator that produced the group.
             */
            evaluator_version?: number | null;
            /**
             * Evaluator Version Id
             * @description Evaluator version that produced the group.
             */
            evaluator_version_id?: string | null;
            /**
             * Name
             * @description Evaluation name.
             */
            name: string;
            /**
             * Replays
             * @description Evaluation values of the 50 most recent replays, oldest first.
             */
            replays: components["schemas"]["ReplayEvaluationValues"][];
            /** @description Stats over the result sessions. */
            result: components["schemas"]["EvaluationStats"];
        };
        /**
         * EvaluationBatchCreateRequest
         * @description Evaluation batch create request.
         */
        EvaluationBatchCreateRequest: {
            /**
             * Evaluators
             * @description Evaluators run against every session.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /**
             * Input Session Ids
             * @description Sessions to score, all belonging to one agent.
             */
            input_session_ids: string[];
        };
        /**
         * EvaluationDataType
         * @description Data type an evaluation result carries.
         * @enum {string}
         */
        EvaluationDataType: "float" | "bool" | "str" | "categorical";
        /**
         * EvaluationResponse
         * @description Evaluation response.
         */
        EvaluationResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /** @description Data type of the result. */
            data_type: components["schemas"]["EvaluationDataType"];
            /**
             * Evaluator Name
             * @description Name of the evaluator that produced the result.
             */
            evaluator_name?: string | null;
            /**
             * Evaluator Params
             * @description Params the evaluator ran with.
             */
            evaluator_params?: {
                [key: string]: unknown;
            } | null;
            /**
             * Evaluator Version
             * @description Version of the evaluator that produced the result.
             */
            evaluator_version?: number | null;
            /**
             * Evaluator Version Id
             * @description Evaluator version that produced the result.
             */
            evaluator_version_id?: string | null;
            /**
             * Explanation
             * @description Free-form explanation.
             */
            explanation?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Evaluation id.
             */
            id: string;
            /**
             * Max Score
             * @description Upper bound of the score scale.
             */
            max_score?: number | null;
            /**
             * Min Score
             * @description Lower bound of the score scale.
             */
            min_score?: number | null;
            /**
             * Name
             * @description Evaluation name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Passed
             * @description Pass or fail verdict.
             */
            passed?: boolean | null;
            /**
             * Score
             * @description Numeric or boolean score.
             */
            score?: number | boolean | null;
            /**
             * Session Id
             * Format: uuid
             * @description Session being scored.
             */
            session_id: string;
            /**
             * Target Score
             * @description Score to beat.
             */
            target_score?: number | null;
            /**
             * Task Id
             * @description Evaluator task that produced the result.
             */
            task_id?: string | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Value
             * @description Label or string value.
             */
            value?: string | null;
        };
        /**
         * EvaluationResult
         * @description Evaluation result.
         */
        EvaluationResult: {
            /**
             * Explanation
             * @description Free-form explanation.
             */
            explanation?: string | null;
            /**
             * Max Score
             * @description Upper bound of the score scale.
             */
            max_score?: number | null;
            /**
             * Min Score
             * @description Lower bound of the score scale.
             */
            min_score?: number | null;
            /**
             * Name
             * @description Evaluation name.
             */
            name: string;
            /**
             * Passed
             * @description Pass or fail verdict.
             */
            passed?: boolean | null;
            /**
             * Score
             * @description Numeric or boolean score.
             */
            score?: number | boolean | null;
            /**
             * Target Score
             * @description Score to beat.
             */
            target_score?: number | null;
            /**
             * Value
             * @description Label or string value.
             */
            value?: string | null;
        };
        /**
         * EvaluationStats
         * @description Evaluation stats.
         */
        EvaluationStats: {
            /**
             * Count
             * @description Number of aggregated evaluations.
             */
            count: number;
            /**
             * Max
             * @description Highest score of float and bool evaluations, null for other data types.
             */
            max?: number | null;
            /**
             * Max Score
             * @description Upper bound of the score scale shared by every aggregated evaluation, null when they differ or one lacks it.
             */
            max_score?: number | null;
            /**
             * Mean
             * @description Mean score of float evaluations, share of true results of bool evaluations, null for other data types.
             */
            mean?: number | null;
            /**
             * Min
             * @description Lowest score of float and bool evaluations, null for other data types.
             */
            min?: number | null;
            /**
             * Min Score
             * @description Lower bound of the score scale shared by every aggregated evaluation, null when they differ or one lacks it.
             */
            min_score?: number | null;
            /**
             * Pass Rate
             * @description Share of passed evaluations among those carrying a passed flag, null when none do.
             */
            pass_rate?: number | null;
            /**
             * Target Score
             * @description Score to beat shared by every aggregated evaluation, null when they differ or one lacks it.
             */
            target_score?: number | null;
            /**
             * Value Counts
             * @description Occurrences per value, only for categorical evaluations.
             */
            value_counts?: {
                [key: string]: number;
            } | null;
        };
        /**
         * EvaluationTaskDetails
         * @description Evaluation task details.
         */
        EvaluationTaskDetails: {
            /**
             * Evaluator Name
             * @description Name the evaluator emits results under.
             */
            evaluator_name: string;
            /**
             * Input Session Id
             * Format: uuid
             * @description Session being scored.
             */
            input_session_id: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            kind: "evaluator";
            /**
             * Params
             * @description Parameters passed to the evaluator.
             */
            params: {
                [key: string]: unknown;
            };
            /**
             * Plugin
             * @description Evaluator plugin to load.
             */
            plugin: components["schemas"]["ScriptPluginSpec"] | components["schemas"]["PackagePluginSpec"];
        };
        /**
         * EvaluationValue
         * @description Evaluation value.
         */
        EvaluationValue: {
            /**
             * Max Score
             * @description Upper bound of the score scale.
             */
            max_score?: number | null;
            /**
             * Min Score
             * @description Lower bound of the score scale.
             */
            min_score?: number | null;
            /**
             * Passed
             * @description Pass or fail verdict.
             */
            passed?: boolean | null;
            /**
             * Score
             * @description Numeric or boolean score.
             */
            score?: number | boolean | null;
            /**
             * Target Score
             * @description Score to beat.
             */
            target_score?: number | null;
            /**
             * Value
             * @description Label or string value.
             */
            value?: string | null;
        };
        /**
         * EvaluatorConfig
         * @description Evaluator config.
         */
        EvaluatorConfig: {
            /**
             * Evaluator
             * @description Evaluator name.
             */
            evaluator: string;
            /**
             * Params
             * @description Parameters passed to the evaluator.
             */
            params?: {
                [key: string]: unknown;
            };
            /**
             * Version
             * @description Evaluator version, an omitted value resolves to latest.
             */
            version?: number | null;
        };
        /**
         * EvaluatorCreateRequest
         * @description Evaluator create request.
         */
        EvaluatorCreateRequest: {
            /**
             * Agent Id
             * @description Id of the agent this evaluator is scoped to, null for a global evaluator.
             */
            agent_id?: string | null;
            /**
             * Description
             * @description Evaluator description.
             */
            description?: string | null;
            /**
             * Logo Url
             * @description Evaluator logo URL.
             */
            logo_url?: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Evaluator name.
             */
            name: string;
        };
        /**
         * EvaluatorResponse
         * @description Evaluator response.
         */
        EvaluatorResponse: {
            /**
             * Agent Id
             * @description Id of the agent this evaluator is scoped to, null for a global evaluator.
             */
            agent_id: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Evaluator description.
             */
            description: string | null;
            /**
             * Id
             * Format: uuid
             * @description Evaluator id.
             */
            id: string;
            /**
             * Latest Version
             * @description Highest version number created for this evaluator.
             */
            latest_version: number;
            /**
             * Logo Url
             * @description Evaluator logo URL.
             */
            logo_url: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Evaluator name.
             */
            name: string;
            /**
             * Owner Id
             * @description Id of the owning account, null for a default plugin.
             */
            owner_id: string | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * EvaluatorUpdateRequest
         * @description Evaluator update request.
         */
        EvaluatorUpdateRequest: {
            /**
             * Description
             * @description New evaluator description.
             */
            description?: string | null;
            /**
             * Logo Url
             * @description New logo URL.
             */
            logo_url?: string | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
        };
        /**
         * EvaluatorVersionCreateRequest
         * @description Evaluator version create request.
         */
        EvaluatorVersionCreateRequest: {
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version?: string | null;
            /**
             * Source
             * @description Evaluator code to load.
             */
            source: components["schemas"]["ScriptPluginSource"] | components["schemas"]["PackagePluginSource"];
        };
        /**
         * EvaluatorVersionResponse
         * @description Evaluator version response.
         */
        EvaluatorVersionResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version: string | null;
            /**
             * Evaluator Id
             * Format: uuid
             * @description Evaluator this version belongs to.
             */
            evaluator_id: string;
            /**
             * Id
             * Format: uuid
             * @description Evaluator version id.
             */
            id: string;
            /**
             * Source
             * @description Evaluator code to load.
             */
            source: components["schemas"]["ScriptPluginSource"] | components["schemas"]["PackagePluginSource"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Version
             * @description Server-assigned version number.
             */
            version: number;
        };
        /**
         * EvaluatorVersionUpdateRequest
         * @description Evaluator version update request.
         */
        EvaluatorVersionUpdateRequest: {
            /**
             * Display Version
             * @description New human-readable designator.
             */
            display_version?: string | null;
        };
        /**
         * ExperimentCreateRequest
         * @description Experiment create request.
         */
        ExperimentCreateRequest: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the experiment's runs belong to.
             */
            agent_id: string;
            /**
             * Description
             * @description Experiment description.
             */
            description?: string | null;
            /**
             * Evaluators
             * @description Evaluators run against every run's replays.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /**
             * Name
             * @description Experiment name.
             */
            name: string;
            /** @description Override applied to every run's replays. */
            override?: components["schemas"]["ReplayOverride"] | null;
            /** @description Tool policy applied to every run's replays. */
            tool_policy?: components["schemas"]["ToolPolicy"] | null;
        };
        /**
         * ExperimentResponse
         * @description Experiment response.
         */
        ExperimentResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the experiment's runs belong to.
             */
            agent_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Experiment description.
             */
            description: string | null;
            /**
             * Evaluators
             * @description Evaluators run against every run's replays.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /**
             * Id
             * Format: uuid
             * @description Experiment id.
             */
            id: string;
            /**
             * Name
             * @description Experiment name.
             */
            name: string;
            /** @description Override applied to every run's replays. */
            override: components["schemas"]["ReplayOverride"] | null;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /** @description Tool policy applied to every run's replays. */
            tool_policy: components["schemas"]["ToolPolicy"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ExperimentRunCreateRequest
         * @description Experiment run create request.
         */
        ExperimentRunCreateRequest: {
            /**
             * Agent Version Id
             * Format: uuid
             * @description Agent version to replay with.
             */
            agent_version_id: string;
            /** @description How to score each baseline session. */
            baseline_evaluation_mode?: components["schemas"]["BaselineEvaluationMode"] | null;
            /**
             * Cohort Version Id
             * Format: uuid
             * @description Cohort version whose sessions are replayed.
             */
            cohort_version_id: string;
            /**
             * Evaluate Baselines
             * @deprecated
             * @description Whether to also score each baseline session.
             * @default false
             */
            evaluate_baselines: boolean;
        };
        /**
         * ExperimentRunProgress
         * @description Experiment run progress.
         */
        ExperimentRunProgress: {
            /**
             * Canceled
             * @description Replays canceled.
             */
            canceled: number;
            /**
             * Completed
             * @description Replays completed.
             */
            completed: number;
            /**
             * Evaluating
             * @description Replays evaluating.
             */
            evaluating: number;
            /**
             * Failed
             * @description Replays failed.
             */
            failed: number;
            /**
             * Pending
             * @description Replays pending.
             */
            pending: number;
            /**
             * Total
             * @description Total replays in the run.
             */
            total: number;
        };
        /**
         * ExperimentRunResponse
         * @description Experiment run response.
         */
        ExperimentRunResponse: {
            /**
             * Agent Version Id
             * Format: uuid
             * @description Agent version to replay with.
             */
            agent_version_id: string;
            /** @description How baseline sessions are scored. */
            baseline_evaluation_mode: components["schemas"]["BaselineEvaluationMode"];
            /**
             * Cohort Version Id
             * Format: uuid
             * @description Cohort version whose sessions are replayed.
             */
            cohort_version_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Ended At
             * @description Time the run settled.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed run.
             */
            error?: string | null;
            /**
             * Evaluate Baselines
             * @deprecated
             * @description Whether baseline sessions are also scored.
             */
            evaluate_baselines: boolean;
            /**
             * Experiment Id
             * Format: uuid
             * @description Experiment this run belongs to.
             */
            experiment_id: string;
            /**
             * Id
             * Format: uuid
             * @description Experiment run id.
             */
            id: string;
            /**
             * Number
             * @description Run number within the experiment.
             */
            number: number;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /** @description Replay counts by status. */
            progress: components["schemas"]["ExperimentRunProgress"];
            /**
             * Started At
             * @description Time the run started.
             */
            started_at?: string | null;
            /** @description Run status. */
            status: components["schemas"]["ExperimentRunStatus"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ExperimentRunStatus
         * @description Experiment run status.
         * @enum {string}
         */
        ExperimentRunStatus: "running" | "canceling" | "completed" | "failed" | "canceled";
        /**
         * ExperimentUpdateRequest
         * @description Experiment update request.
         */
        ExperimentUpdateRequest: {
            /**
             * Description
             * @description New experiment description.
             */
            description?: string | null;
            /**
             * Evaluators
             * @description New evaluators.
             */
            evaluators?: components["schemas"]["EvaluatorConfig"][] | null;
            /**
             * Name
             * @description New experiment name.
             */
            name?: string | null;
            /** @description New override. */
            override?: components["schemas"]["ReplayOverride"] | null;
            /** @description New tool policy. */
            tool_policy?: components["schemas"]["ToolPolicy"] | null;
        };
        /**
         * FilterCondition
         * @description Filter condition.
         */
        FilterCondition: {
            /**
             * Field
             * @description Field to filter on.
             */
            field: string;
            /** @description Comparison operator. */
            op: components["schemas"]["FilterOp"];
            /**
             * Value
             * @description Comparison value.
             */
            value?: unknown;
        };
        /**
         * FilterOp
         * @description Filter condition operator.
         * @enum {string}
         */
        FilterOp: "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "in" | "is_null" | "startswith" | "endswith" | "contains";
        /**
         * HistoryConfig
         * @description History tool config.
         */
        HistoryConfig: {
            /** @description Behavior when no recorded call matches. */
            on_miss: components["schemas"]["ToolPolicyOnMiss"];
            /** @description Source of recorded calls to replay from. */
            scope: components["schemas"]["HistoryScope"];
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "history";
        };
        /**
         * HistoryScope
         * @description Scope a history tool config draws recorded calls from.
         * @enum {string}
         */
        HistoryScope: "baseline" | "cohort_version" | "agent";
        /**
         * ImportCreateRequest
         * @description Import create request.
         */
        ImportCreateRequest: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent imported sessions are created under.
             */
            agent_id: string;
            /**
             * Agent Version Id
             * @description Agent version recorded on the imported sessions.
             */
            agent_version_id?: string | null;
            /**
             * Evaluators
             * @description Evaluators run against every imported session.
             */
            evaluators?: components["schemas"]["EvaluatorConfig"][];
            /**
             * Importer
             * @description Importer name.
             */
            importer: string;
            /**
             * Params
             * @description Parameters passed to the importer.
             */
            params?: {
                [key: string]: unknown;
            };
            /**
             * Payload Blob Id
             * Format: uuid
             * @description Blob holding the payload to parse.
             */
            payload_blob_id: string;
            /**
             * Version
             * @description Importer version, an omitted value resolves to latest.
             */
            version?: number | null;
        };
        /**
         * ImportFailure
         * @description Import failure.
         */
        ImportFailure: {
            /**
             * Error
             * @description Failure reason.
             */
            error: string;
            /**
             * External Id
             * @description External id of the failed item.
             */
            external_id?: string | null;
            /**
             * Line
             * @description Line the failure occurred at.
             */
            line: number;
        };
        /**
         * ImportResponse
         * @description Import response.
         */
        ImportResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent imported sessions are created under.
             */
            agent_id: string;
            /**
             * Agent Version Id
             * @description Agent version recorded on the imported sessions.
             */
            agent_version_id?: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Error
             * @description Error from a failed import.
             */
            error?: string | null;
            /**
             * Evaluators
             * @description Evaluators run against every imported session.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /**
             * Id
             * Format: uuid
             * @description Import id.
             */
            id: string;
            /**
             * Importer Version Id
             * Format: uuid
             * @description Importer version run.
             */
            importer_version_id: string;
            /**
             * Job Id
             * @description Job running the import.
             */
            job_id?: string | null;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Params
             * @description Parameters passed to the importer.
             */
            params: {
                [key: string]: unknown;
            };
            /**
             * Payload Blob Id
             * Format: uuid
             * @description Blob holding the payload parsed.
             */
            payload_blob_id: string;
            /** @description Stats from a completed import. */
            stats?: components["schemas"]["ImportStats"] | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ImportStats
         * @description Import stats.
         */
        ImportStats: {
            /**
             * Created
             * @description Sessions created.
             */
            created: number;
            /**
             * Failed
             * @description Items that failed to import.
             */
            failed: number;
            /**
             * Failures
             * @description Sample of failures.
             */
            failures?: components["schemas"]["ImportFailure"][];
            /**
             * Skipped
             * @description Sessions skipped as duplicates.
             */
            skipped: number;
        };
        /**
         * ImportTaskDetails
         * @description Import task details.
         */
        ImportTaskDetails: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent imported sessions are created under.
             */
            agent_id: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            kind: "importer";
            /**
             * Params
             * @description Parameters passed to the importer.
             */
            params: {
                [key: string]: unknown;
            };
            /** @description Payload to parse. */
            payload: components["schemas"]["PayloadSpec"];
            /**
             * Plugin
             * @description Importer plugin to load.
             */
            plugin: components["schemas"]["ScriptPluginSpec"] | components["schemas"]["PackagePluginSpec"];
            /**
             * Provider
             * @description Source system named on the import.
             */
            provider?: string | null;
        };
        /**
         * ImporterCreateRequest
         * @description Importer create request.
         */
        ImporterCreateRequest: {
            /**
             * Description
             * @description Importer description.
             */
            description?: string | null;
            /**
             * Logo Url
             * @description Importer logo URL.
             */
            logo_url?: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Importer name.
             */
            name: string;
            /**
             * Provider
             * @description Source system this importer reads.
             */
            provider?: string | null;
        };
        /**
         * ImporterResponse
         * @description Importer response.
         */
        ImporterResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Importer description.
             */
            description: string | null;
            /**
             * Id
             * Format: uuid
             * @description Importer id.
             */
            id: string;
            /**
             * Latest Version
             * @description Highest version number created for this importer.
             */
            latest_version: number;
            /**
             * Logo Url
             * @description Importer logo URL.
             */
            logo_url: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Importer name.
             */
            name: string;
            /**
             * Owner Id
             * @description Id of the owning account, null for a default plugin.
             */
            owner_id: string | null;
            /**
             * Provider
             * @description Source system this importer reads.
             */
            provider: string | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ImporterUpdateRequest
         * @description Importer update request.
         */
        ImporterUpdateRequest: {
            /**
             * Description
             * @description New importer description.
             */
            description?: string | null;
            /**
             * Logo Url
             * @description New logo URL.
             */
            logo_url?: string | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
        };
        /**
         * ImporterVersionCreateRequest
         * @description Importer version create request.
         */
        ImporterVersionCreateRequest: {
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version?: string | null;
            /**
             * Source
             * @description Importer code to load.
             */
            source: components["schemas"]["ScriptPluginSource"] | components["schemas"]["PackagePluginSource"];
        };
        /**
         * ImporterVersionResponse
         * @description Importer version response.
         */
        ImporterVersionResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Display Version
             * @description Human-readable designator.
             */
            display_version: string | null;
            /**
             * Id
             * Format: uuid
             * @description Importer version id.
             */
            id: string;
            /**
             * Importer Id
             * Format: uuid
             * @description Importer this version belongs to.
             */
            importer_id: string;
            /**
             * Source
             * @description Importer code to load.
             */
            source: components["schemas"]["ScriptPluginSource"] | components["schemas"]["PackagePluginSource"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Version
             * @description Server-assigned version number.
             */
            version: number;
        };
        /**
         * ImporterVersionUpdateRequest
         * @description Importer version update request.
         */
        ImporterVersionUpdateRequest: {
            /**
             * Display Version
             * @description New human-readable designator.
             */
            display_version?: string | null;
        };
        /**
         * InsightBatchCreateRequest
         * @description Insight batch create request.
         */
        InsightBatchCreateRequest: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the insights belong to.
             */
            agent_id: string;
            /**
             * Insights
             * @description Insights to create, in input order.
             */
            insights: components["schemas"]["InsightInput"][];
        };
        /**
         * InsightInput
         * @description Insight input.
         */
        InsightInput: {
            /**
             * Data
             * @description Insight data.
             */
            data: components["schemas"]["TextInsightData"] | components["schemas"]["CategoricalInsightData"] | components["schemas"]["BinnedInsightData"];
            /**
             * Description
             * @description Insight description.
             */
            description?: string | null;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Insight name.
             */
            name: string;
            /**
             * Title
             * @description Insight title.
             */
            title: string;
        };
        /**
         * InsightResponse
         * @description Insight response.
         */
        InsightResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the insight belongs to.
             */
            agent_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Data
             * @description Insight data.
             */
            data: components["schemas"]["TextInsightData"] | components["schemas"]["CategoricalInsightData"] | components["schemas"]["BinnedInsightData"];
            /**
             * Description
             * @description Insight description.
             */
            description: string | null;
            /**
             * Id
             * Format: uuid
             * @description Insight id.
             */
            id: string;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Insight name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Title
             * @description Insight title.
             */
            title: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * InsightUpdateRequest
         * @description Insight update request.
         */
        InsightUpdateRequest: {
            /**
             * Description
             * @description New insight description.
             */
            description?: string | null;
            /**
             * Title
             * @description New insight title.
             */
            title?: string | null;
        };
        /**
         * InvestigationAnswerCreateRequest
         * @description Investigation answer create request.
         */
        InvestigationAnswerCreateRequest: {
            /**
             * Investigation Session Id
             * Format: uuid
             * @description Investigation session being answered.
             */
            investigation_session_id: string;
            /**
             * Question Key
             * @description Key of the question being answered.
             */
            question_key: string;
            /** @description Part of the session being annotated. */
            selector?: components["schemas"]["AnnotationSelector"] | null;
            /**
             * Value
             * @description Annotation value.
             */
            value: unknown;
        };
        /**
         * InvestigationCreateRequest
         * @description Investigation create request.
         */
        InvestigationCreateRequest: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the investigation's sessions belong to.
             */
            agent_id: string;
            /**
             * Description
             * @description Curator rationale.
             */
            description?: string | null;
            /**
             * Name
             * @description Investigation name.
             */
            name: string;
            /**
             * Sessions
             * @description Sessions to investigate, in presentation order.
             */
            sessions: components["schemas"]["InvestigationSessionInput"][];
        };
        /**
         * InvestigationResponse
         * @description Investigation response.
         */
        InvestigationResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the investigation's sessions belong to.
             */
            agent_id: string;
            /**
             * Completed Sessions
             * @description Number of linked sessions with a verdict.
             */
            completed_sessions: number;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Description
             * @description Curator rationale.
             */
            description: string | null;
            /**
             * Ended At
             * @description Time the last session settled.
             */
            ended_at?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Investigation id.
             */
            id: string;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Investigation name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Started At
             * @description Time the first answer was recorded.
             */
            started_at?: string | null;
            status: components["schemas"]["InvestigationStatus"];
            /**
             * Total Sessions
             * @description Number of linked sessions.
             */
            total_sessions: number;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * InvestigationSessionHighlight
         * @description Investigation session highlight.
         */
        InvestigationSessionHighlight: {
            /**
             * Description
             * @description Prose explaining what the highlight shows.
             */
            description: string;
            /** @description Part of the session highlighted. */
            selector: components["schemas"]["AnnotationSelector"];
        };
        /**
         * InvestigationSessionInput
         * @description Investigation session input.
         */
        InvestigationSessionInput: {
            /**
             * Questions
             * @description Questions to answer about the session.
             */
            questions: components["schemas"]["InvestigationSessionQuestion"][];
            /**
             * Session Id
             * Format: uuid
             * @description Session to link.
             */
            session_id: string;
        };
        /**
         * InvestigationSessionQuestion
         * @description Investigation session question.
         */
        InvestigationSessionQuestion: {
            /**
             * Highlights
             * @description Curated highlights for the question.
             */
            highlights?: components["schemas"]["InvestigationSessionHighlight"][];
            /**
             * Key
             * @description Question key, unique within the session.
             */
            key: string;
            /**
             * Question
             * @description Question to answer about the session.
             */
            question: string;
        };
        /**
         * InvestigationSessionResponse
         * @description Investigation session response.
         */
        InvestigationSessionResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Investigation session id.
             */
            id: string;
            /**
             * Investigation Id
             * Format: uuid
             * @description Investigation this session belongs to.
             */
            investigation_id: string;
            /**
             * Position
             * @description Presentation order within the investigation.
             */
            position: number;
            /**
             * Questions
             * @description Questions to answer about the session.
             */
            questions: components["schemas"]["InvestigationSessionQuestion"][];
            /**
             * Session Id
             * Format: uuid
             * @description Session being investigated.
             */
            session_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /** @description Investigation session verdict. */
            verdict: components["schemas"]["InvestigationSessionVerdict"] | null;
        };
        /**
         * InvestigationSessionUpdateRequest
         * @description Investigation session update request.
         */
        InvestigationSessionUpdateRequest: {
            /** @description New investigation session verdict, None clears it. */
            verdict: components["schemas"]["InvestigationSessionVerdict"] | null;
        };
        /**
         * InvestigationSessionVerdict
         * @description Investigation session verdict.
         * @enum {string}
         */
        InvestigationSessionVerdict: "acceptable" | "problematic" | "uncertain";
        /**
         * InvestigationStatus
         * @description Investigation status.
         * @enum {string}
         */
        InvestigationStatus: "pending" | "in_progress" | "completed";
        /**
         * InvestigationUpdateRequest
         * @description Investigation update request.
         */
        InvestigationUpdateRequest: {
            /**
             * Description
             * @description New curator rationale.
             */
            description?: string | null;
            /**
             * Name
             * @description New investigation name.
             */
            name?: string | null;
            /** @description New investigation status. */
            status?: components["schemas"]["InvestigationStatus"] | null;
        };
        /**
         * JobKind
         * @description Job kind.
         * @enum {string}
         */
        JobKind: "session_run" | "import" | "evaluation" | "replay";
        /**
         * JobResponse
         * @description Job response.
         */
        JobResponse: {
            /**
             * Cancel Requested At
             * @description Time cancellation was requested.
             */
            cancel_requested_at?: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Ended At
             * @description Time the job settled.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description First counted task failure's error.
             */
            error?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Job id.
             */
            id: string;
            /** @description Kind of workflow that created the job. */
            kind: components["schemas"]["JobKind"];
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Started At
             * @description Time the job started.
             */
            started_at?: string | null;
            status: components["schemas"]["JobStatus"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * JobStatus
         * @description Job status.
         * @enum {string}
         */
        JobStatus: "pending" | "running" | "completed" | "failed" | "canceled";
        /**
         * LLMConfig
         * @description LLM tool config.
         */
        LLMConfig: {
            /**
             * Instructions
             * @description Instructions guiding the generated result.
             */
            instructions?: string | null;
            /**
             * Model
             * @description Model used to generate the tool result.
             */
            model: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "llm";
        };
        /**
         * LabelSelector
         * @description Label selector.
         */
        LabelSelector: {
            /**
             * Key
             * @description Label key.
             */
            key: string;
            /**
             * Required
             * @description Whether a task lacking the key fails the match.
             * @default false
             */
            required: boolean;
            /**
             * Values
             * @description Values the label may take.
             */
            values: string[];
        };
        /**
         * ManualAnnotationCreateRequest
         * @description Manual annotation create request.
         */
        ManualAnnotationCreateRequest: {
            /** @description Part of the session being annotated. */
            selector?: components["schemas"]["AnnotationSelector"] | null;
            /**
             * Session Id
             * Format: uuid
             * @description Session being annotated.
             */
            session_id: string;
            /**
             * Value
             * @description Annotation value.
             */
            value: unknown;
        };
        /**
         * NodeStatus
         * @description Session node status.
         * @enum {string}
         */
        NodeStatus: "in_progress" | "completed" | "failed";
        /**
         * NodeType
         * @description Kind of work a session node records.
         * @enum {string}
         */
        NodeType: "llm_call" | "tool_call" | "subagent_call" | "span";
        /**
         * NotFilter
         * @description Not filter.
         */
        NotFilter: {
            /**
             * Not
             * @description Operand.
             */
            not: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"];
        };
        /**
         * OrFilter
         * @description Or filter.
         */
        OrFilter: {
            /**
             * Or
             * @description Operands.
             */
            or: (components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"])[];
        };
        /**
         * PackagePluginSource
         * @description Package plugin source.
         */
        PackagePluginSource: {
            /**
             * Entrypoint
             * @description Module and attribute, as module:attribute.
             */
            entrypoint: string;
            /**
             * Requirement
             * @description Pinned PEP 508 requirement.
             */
            requirement: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "package";
        };
        /**
         * PackagePluginSpec
         * @description Package plugin spec.
         */
        PackagePluginSpec: {
            /**
             * Entrypoint
             * @description Module and attribute, as module:attribute.
             */
            entrypoint: string;
            /**
             * Requirement
             * @description Pinned PEP 508 requirement.
             */
            requirement: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "package";
        };
        /** Page[AccountResponse] */
        Page_AccountResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["AccountResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[AgentResponse] */
        Page_AgentResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["AgentResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[AgentVersionResponse] */
        Page_AgentVersionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["AgentVersionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[AnnotationResponse] */
        Page_AnnotationResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["AnnotationResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ApiKeyResponse] */
        Page_ApiKeyResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ApiKeyResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[CohortResponse] */
        Page_CohortResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["CohortResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[CohortVersionResponse] */
        Page_CohortVersionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["CohortVersionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[DeviceResponse] */
        Page_DeviceResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["DeviceResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[EvaluationResponse] */
        Page_EvaluationResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["EvaluationResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[EvaluatorResponse] */
        Page_EvaluatorResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["EvaluatorResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[EvaluatorVersionResponse] */
        Page_EvaluatorVersionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["EvaluatorVersionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ExperimentResponse] */
        Page_ExperimentResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ExperimentResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ExperimentRunResponse] */
        Page_ExperimentRunResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ExperimentRunResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ImportResponse] */
        Page_ImportResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ImportResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ImporterResponse] */
        Page_ImporterResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ImporterResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ImporterVersionResponse] */
        Page_ImporterVersionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ImporterVersionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[InsightResponse] */
        Page_InsightResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["InsightResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[InvestigationResponse] */
        Page_InvestigationResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["InvestigationResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[InvestigationSessionResponse] */
        Page_InvestigationSessionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["InvestigationSessionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[JobResponse] */
        Page_JobResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["JobResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[ReplayResponse] */
        Page_ReplayResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["ReplayResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[SecretResponse] */
        Page_SecretResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["SecretResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[SessionDetailResponse] */
        Page_SessionDetailResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["SessionDetailResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[SessionNodeResponse] */
        Page_SessionNodeResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["SessionNodeResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[SessionResponse] */
        Page_SessionResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["SessionResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[SessionWithEvaluationsResponse] */
        Page_SessionWithEvaluationsResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["SessionWithEvaluationsResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[TagResponse] */
        Page_TagResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["TagResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[TaskResponse] */
        Page_TaskResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["TaskResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /** Page[WorkerResponse] */
        Page_WorkerResponse_: {
            /**
             * Items
             * @description Items on this page.
             */
            items: components["schemas"]["WorkerResponse"][];
            /**
             * Next Cursor
             * @description Cursor for the next page, null on the last page.
             */
            next_cursor: string | null;
        };
        /**
         * PassthroughConfig
         * @description Passthrough tool config.
         */
        PassthroughConfig: {
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "passthrough";
        };
        /**
         * PayloadSpec
         * @description Payload spec.
         */
        PayloadSpec: {
            /**
             * Blob Id
             * Format: uuid
             * @description Blob holding the payload.
             */
            blob_id: string;
            /**
             * Sha256
             * @description Blob content hash.
             */
            sha256: string;
        };
        /**
         * ReplayCreateRequest
         * @description Replay create request.
         */
        ReplayCreateRequest: {
            /**
             * Agent Version Id
             * @description Agent version to replay with, the baseline session's recorded version when unset.
             */
            agent_version_id?: string | null;
            /** @description How to score the baseline session. */
            baseline_evaluation_mode?: components["schemas"]["BaselineEvaluationMode"] | null;
            /**
             * Baseline Session Id
             * Format: uuid
             * @description Session to replay.
             */
            baseline_session_id: string;
            /**
             * Evaluate Baselines
             * @deprecated
             * @description Whether to also score the baseline session.
             * @default false
             */
            evaluate_baselines: boolean;
            /**
             * Evaluators
             * @description Evaluators run against the result session.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /** @description Override to apply. */
            override?: components["schemas"]["ReplayOverride"] | null;
            /** @description Tool policy to apply. */
            tool_policy?: components["schemas"]["ToolPolicy"] | null;
        };
        /**
         * ReplayEvaluationValues
         * @description Replay evaluation values.
         */
        ReplayEvaluationValues: {
            /** @description Value from the baseline session. */
            baseline?: components["schemas"]["EvaluationValue"] | null;
            /**
             * Replay Id
             * Format: uuid
             * @description Replay id.
             */
            replay_id: string;
            /** @description Value from the result session. */
            result?: components["schemas"]["EvaluationValue"] | null;
        };
        /**
         * ReplayOverride
         * @description Replay override.
         */
        ReplayOverride: {
            /**
             * Model
             * @description New model, or a map from old to new model.
             */
            model?: string | {
                [key: string]: string;
            } | null;
            /**
             * Model Params
             * @description New model parameters.
             */
            model_params?: {
                [key: string]: unknown;
            } | null;
            /**
             * Prompt
             * @description New prompt.
             */
            prompt?: string | null;
            /**
             * System Prompt
             * @description New system prompt.
             */
            system_prompt?: string | null;
        };
        /**
         * ReplayResponse
         * @description Replay response.
         */
        ReplayResponse: {
            /** @description How the baseline session is scored. */
            baseline_evaluation_mode: components["schemas"]["BaselineEvaluationMode"];
            /**
             * Baseline Session Id
             * Format: uuid
             * @description Session replayed.
             */
            baseline_session_id: string;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Error
             * @description Error from a failed replay.
             */
            error?: string | null;
            /**
             * Evaluate Baselines
             * @deprecated
             * @description Whether the baseline session is also scored.
             */
            evaluate_baselines: boolean;
            /**
             * Evaluators
             * @description Evaluators run against the result session.
             */
            evaluators: components["schemas"]["EvaluatorConfig"][];
            /**
             * Experiment Run Id
             * @description Experiment run this replay belongs to.
             */
            experiment_run_id?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Replay id.
             */
            id: string;
            /**
             * Job Id
             * @description Job running the replay.
             */
            job_id?: string | null;
            /** @description Override applied. */
            override: components["schemas"]["ReplayOverride"] | null;
            /**
             * Result Session Id
             * @description Session produced by the replay.
             */
            result_session_id?: string | null;
            status: components["schemas"]["ReplayStatus"];
            /** @description Tool policy applied. */
            tool_policy: components["schemas"]["ToolPolicy"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * ReplayStatus
         * @description Replay status.
         * @enum {string}
         */
        ReplayStatus: "pending" | "evaluating" | "completed" | "failed" | "canceled";
        /**
         * RunSpec
         * @description Run spec.
         */
        RunSpec: {
            /**
             * Command
             * @description Shell command to run.
             */
            command: string;
            /**
             * Env
             * @description Process environment.
             */
            env?: {
                [key: string]: string;
            };
            /**
             * Hooks
             * @description Hooks run around the task process.
             */
            hooks?: (components["schemas"]["CopyWorkdirHook"] | components["schemas"]["SetupCommandHook"] | components["schemas"]["TeardownCommandHook"])[];
            runtime_capabilities?: components["schemas"]["RuntimeCapabilities"];
            /**
             * Secret Ids
             * @description Secrets merged into the process environment.
             */
            secret_ids?: string[];
            /**
             * Timeout Seconds
             * @description Process timeout.
             * @default 3600
             */
            timeout_seconds: number;
            /**
             * Working Dir
             * @description Working directory.
             */
            working_dir?: string | null;
        };
        /**
         * RuntimeCapabilities
         * @description Runtime capabilities.
         */
        RuntimeCapabilities: {
            /**
             * Overrides
             * @description Whether the runtime can apply replay overrides.
             * @default true
             */
            overrides: boolean;
            /**
             * Tool Policies
             * @description Whether the runtime can apply non-passthrough tool policies.
             * @default true
             */
            tool_policies: boolean;
        };
        /**
         * SampleDataCreateRequest
         * @description Sample data create request.
         */
        SampleDataCreateRequest: {
            /**
             * Agent Name
             * @description Agent name, None uses the sample data's agent name.
             */
            agent_name?: string | null;
        };
        /**
         * SampleDataResponse
         * @description Sample data response.
         */
        SampleDataResponse: {
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the sample data was seeded under.
             */
            agent_id: string;
        };
        /**
         * ScriptPluginSource
         * @description Script plugin source.
         */
        ScriptPluginSource: {
            /**
             * Blob Id
             * Format: uuid
             * @description Blob holding the script.
             */
            blob_id: string;
            /**
             * Entrypoint
             * @description Attribute in the file.
             */
            entrypoint: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "script";
        };
        /**
         * ScriptPluginSpec
         * @description Script plugin spec.
         */
        ScriptPluginSpec: {
            /**
             * Blob Id
             * Format: uuid
             * @description Blob holding the script.
             */
            blob_id: string;
            /**
             * Entrypoint
             * @description Attribute in the file.
             */
            entrypoint: string;
            /**
             * Sha256
             * @description Blob content hash.
             */
            sha256: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "script";
        };
        /**
         * SecretCreateRequest
         * @description Secret create request.
         */
        SecretCreateRequest: {
            /**
             * Name
             * @description Secret name.
             */
            name: string;
            /**
             * Type
             * @description Secret type.
             */
            type?: string | null;
            /**
             * Values
             * @description Secret values.
             */
            values: {
                [key: string]: string;
            };
        };
        /**
         * SecretResponse
         * @description Secret response.
         */
        SecretResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Secret id.
             */
            id: string;
            /**
             * Name
             * @description Secret name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Type
             * @description Secret type.
             */
            type: string | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * SecretUpdateRequest
         * @description Secret update request.
         */
        SecretUpdateRequest: {
            /**
             * Type
             * @description New secret type.
             */
            type?: string | null;
            /**
             * Values
             * @description New secret values.
             */
            values?: {
                [key: string]: string;
            } | null;
        };
        /**
         * SecretWithValuesResponse
         * @description Secret response carrying the secret values.
         */
        SecretWithValuesResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Secret id.
             */
            id: string;
            /**
             * Name
             * @description Secret name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Type
             * @description Secret type.
             */
            type: string | null;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Values
             * @description Secret values.
             */
            values: {
                [key: string]: string;
            };
        };
        /**
         * ServerInfoResponse
         * @description Server info response.
         */
        ServerInfoResponse: {
            /** @description Scheme used to authenticate requests. */
            auth_scheme: components["schemas"]["AuthScheme"];
            /**
             * Control Plane Api Url
             * @description Control plane API the server accepts credentials from.
             */
            control_plane_api_url?: string | null;
            /**
             * Dashboard Url
             * @description URL the dashboard is reachable at.
             */
            dashboard_url?: string | null;
            /**
             * Id
             * @description Server ID.
             */
            id?: string | null;
            /**
             * Server Url
             * @description URL the server API is reachable at.
             */
            server_url?: string | null;
            /**
             * Ui Version
             * @description Kitaru UI version the server serves.
             */
            ui_version?: string | null;
            /**
             * Version
             * @description Kitaru version the server runs.
             */
            version: string;
        };
        /**
         * ServiceAccountCreateRequest
         * @description Service account create request.
         */
        ServiceAccountCreateRequest: {
            /**
             * Email
             * @description Contact email.
             */
            email?: string | null;
            /**
             * Name
             * @description Account name.
             */
            name: string;
        };
        /**
         * ServiceAccountUpdateRequest
         * @description Service account update request.
         */
        ServiceAccountUpdateRequest: {
            /**
             * Active
             * @description New active state.
             */
            active?: boolean | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
        };
        /**
         * SessionCreateRequest
         * @description Session create request.
         */
        SessionCreateRequest: {
            /**
             * Adapter Version
             * @description Recording adapter version.
             */
            adapter_version?: string | null;
            /**
             * Agent Id
             * @description Agent the session belongs to, inferred from the task or the agent version when unset.
             */
            agent_id?: string | null;
            /**
             * Agent Version Id
             * @description Agent version recorded for the session, inferred from the task when unset.
             */
            agent_version_id?: string | null;
            /**
             * Ended At
             * @description Time the session ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed session.
             */
            error?: string | null;
            /**
             * External Id
             * @description Id from the source system.
             */
            external_id?: string | null;
            /**
             * Framework
             * @description Agent framework used.
             */
            framework?: string | null;
            /**
             * Imported From
             * @description Source system the session was imported from.
             */
            imported_from?: string | null;
            /**
             * Input Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from session inputs.
             */
            input_text_selector?: string | null;
            /**
             * Inputs
             * @description Session inputs.
             */
            inputs: unknown;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Session name.
             */
            name?: string | null;
            /** @description How the session came to exist. */
            origin: components["schemas"]["SessionOrigin"];
            /**
             * Output Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from session outputs.
             */
            output_text_selector?: string | null;
            /**
             * Outputs
             * @description Session outputs.
             */
            outputs: unknown;
            /**
             * Started At
             * @description Time the session started.
             */
            started_at?: string | null;
            /** @description Initial session status. */
            status?: components["schemas"]["SessionStatus"] | null;
        };
        /**
         * SessionDetailResponse
         * @description Session detail response.
         */
        SessionDetailResponse: {
            /**
             * Adapter Version
             * @description Recording adapter version.
             */
            adapter_version?: string | null;
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the session belongs to.
             */
            agent_id: string;
            /**
             * Agent Version Id
             * @description Agent version recorded for the session.
             */
            agent_version_id?: string | null;
            /**
             * Cost
             * @description Total cost.
             */
            cost?: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Ended At
             * @description Time the session ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed session.
             */
            error?: string | null;
            /**
             * External Id
             * @description Id from the source system.
             */
            external_id?: string | null;
            /**
             * Framework
             * @description Agent framework used.
             */
            framework?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Session id.
             */
            id: string;
            /**
             * Import Id
             * @description Import the session was created by.
             */
            import_id?: string | null;
            /**
             * Imported From
             * @description Source system the session was imported from.
             */
            imported_from?: string | null;
            /**
             * Input Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from session inputs.
             */
            input_text_selector?: string | null;
            /**
             * Inputs
             * @description Session inputs.
             */
            inputs: unknown;
            /**
             * Llm Call Count
             * @description Number of LLM call nodes.
             */
            llm_call_count: number;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Session name.
             */
            name?: string | null;
            /**
             * Number
             * @description Session number within the agent.
             */
            number: number;
            /** @description How the session came to exist. */
            origin: components["schemas"]["SessionOrigin"];
            /**
             * Output Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from session outputs.
             */
            output_text_selector?: string | null;
            /**
             * Outputs
             * @description Session outputs.
             */
            outputs: unknown;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Started At
             * @description Time the session started.
             */
            started_at?: string | null;
            status: components["schemas"]["SessionStatus"];
            /**
             * Task Id
             * @description Task the session was produced by.
             */
            task_id?: string | null;
            /** @description Total token usage. */
            tokens?: components["schemas"]["TokenUsage"] | null;
            /**
             * Tool Call Count
             * @description Number of tool call nodes.
             */
            tool_call_count: number;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * SessionDetailWithEvaluationsResponse
         * @description Session detail with evaluations response.
         */
        SessionDetailWithEvaluationsResponse: {
            /**
             * Evaluations
             * @description Every evaluation of the session, newest first.
             */
            evaluations: components["schemas"]["EvaluationResponse"][];
            /** @description Session. */
            session: components["schemas"]["SessionDetailResponse"];
        };
        /**
         * SessionEvaluationsRequest
         * @description Session evaluations request.
         */
        SessionEvaluationsRequest: {
            /**
             * Evaluations
             * @description Evaluations to merge into the session.
             */
            evaluations: components["schemas"]["EvaluationResult"][];
        };
        /**
         * SessionNodeBatchRequest
         * @description Session node batch request.
         */
        SessionNodeBatchRequest: {
            /**
             * Nodes
             * @description Nodes to upsert, parent before child.
             */
            nodes: components["schemas"]["SessionNodeCreateRequest"][];
        };
        /**
         * SessionNodeCreateRequest
         * @description Session node create request.
         */
        SessionNodeCreateRequest: {
            /**
             * Attributes
             * @description Arbitrary span attributes.
             */
            attributes: unknown;
            /**
             * Cost
             * @description Cost of the call.
             */
            cost?: number | string | null;
            /**
             * Ended At
             * @description Time the node ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed node.
             */
            error?: string | null;
            /**
             * External Id
             * @description Id from the source system.
             */
            external_id?: string | null;
            /**
             * Index
             * @description Position within the session, the wire identity.
             */
            index: number;
            /**
             * Input Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from node inputs.
             */
            input_text_selector?: string | null;
            /**
             * Inputs
             * @description Node inputs.
             */
            inputs: unknown;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: unknown;
            };
            /**
             * Model
             * @description Model that served the call.
             */
            model?: string | null;
            /**
             * Model Params
             * @description Parameters passed to the model.
             */
            model_params?: {
                [key: string]: unknown;
            } | null;
            /**
             * Model Provider
             * @description Model provider.
             */
            model_provider?: string | null;
            /**
             * Name
             * @description Node name.
             */
            name: string;
            /** @description Kind of work the node records. */
            node_type: components["schemas"]["NodeType"];
            /**
             * Output Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from node outputs.
             */
            output_text_selector?: string | null;
            /**
             * Outputs
             * @description Node outputs.
             */
            outputs: unknown;
            /**
             * Parent Index
             * @description Index of the parent node.
             */
            parent_index?: number | null;
            /**
             * Reasoning
             * @description Visible reasoning produced by the model call.
             */
            reasoning?: string | null;
            /**
             * Requested Model
             * @description Model requested by the call.
             */
            requested_model?: string | null;
            /**
             * Secondary Parent Indexes
             * @description Indexes of additional parent nodes.
             */
            secondary_parent_indexes?: number[];
            /**
             * Started At
             * @description Time the node started.
             */
            started_at?: string | null;
            /** @description Node status. */
            status: components["schemas"]["NodeStatus"];
            /**
             * Subagent Id
             * @description Subagent invoked.
             */
            subagent_id?: string | null;
            /**
             * System Prompt Selector
             * @description RFC 6901 JSON Pointer selecting the system prompt from node inputs.
             */
            system_prompt_selector?: string | null;
            /** @description Token usage. */
            tokens?: components["schemas"]["TokenUsage"] | null;
            /**
             * Tool Name
             * @description Tool called.
             */
            tool_name?: string | null;
            /**
             * Trace Id
             * @description Distributed trace id.
             */
            trace_id?: string | null;
        };
        /**
         * SessionNodeResponse
         * @description Session node response.
         */
        SessionNodeResponse: {
            /**
             * Attributes
             * @description Arbitrary span attributes, null unless include_payloads.
             */
            attributes?: unknown;
            /**
             * Cache Key
             * @description Cache key for a replayed tool call.
             */
            cache_key?: string | null;
            /**
             * Cost
             * @description Cost of the call.
             */
            cost?: string | null;
            /**
             * Ended At
             * @description Time the node ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed node.
             */
            error?: string | null;
            /**
             * External Id
             * @description Id from the source system.
             */
            external_id?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Node id.
             */
            id: string;
            /**
             * Index
             * @description Position within the session.
             */
            index: number;
            /**
             * Input Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from node inputs.
             */
            input_text_selector?: string | null;
            /**
             * Inputs
             * @description Node inputs, null unless include_payloads.
             */
            inputs?: unknown;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Model
             * @description Model that served the call.
             */
            model?: string | null;
            /**
             * Model Params
             * @description Parameters passed to the model.
             */
            model_params?: {
                [key: string]: unknown;
            } | null;
            /**
             * Model Provider
             * @description Model provider.
             */
            model_provider?: string | null;
            /**
             * Name
             * @description Node name.
             */
            name: string;
            /** @description Kind of work the node records. */
            node_type: components["schemas"]["NodeType"];
            /**
             * Output Text Selector
             * @description RFC 6901 JSON Pointer selecting display text from node outputs.
             */
            output_text_selector?: string | null;
            /**
             * Outputs
             * @description Node outputs, null unless include_payloads.
             */
            outputs?: unknown;
            /**
             * Parent Id
             * @description Parent node.
             */
            parent_id?: string | null;
            /**
             * Parent Index
             * @description Parent node index.
             */
            parent_index: number | null;
            /**
             * Reasoning
             * @description Visible reasoning, null unless payloads are included.
             */
            reasoning?: string | null;
            /**
             * Requested Model
             * @description Model requested by the call.
             */
            requested_model?: string | null;
            /**
             * Secondary Parent Ids
             * @description Additional parent nodes.
             */
            secondary_parent_ids: string[];
            /**
             * Secondary Parent Indexes
             * @description Secondary parent indexes.
             */
            secondary_parent_indexes: number[];
            /**
             * Session Id
             * Format: uuid
             * @description Session this node belongs to.
             */
            session_id: string;
            /**
             * Started At
             * @description Time the node started.
             */
            started_at?: string | null;
            /** @description Node status. */
            status: components["schemas"]["NodeStatus"];
            /**
             * Subagent Id
             * @description Subagent invoked.
             */
            subagent_id?: string | null;
            /**
             * System Prompt Selector
             * @description RFC 6901 JSON Pointer selecting the system prompt from node inputs.
             */
            system_prompt_selector?: string | null;
            /** @description Token usage. */
            tokens?: components["schemas"]["TokenUsage"] | null;
            /**
             * Tool Name
             * @description Tool called.
             */
            tool_name?: string | null;
            /**
             * Trace Id
             * @description Distributed trace id.
             */
            trace_id?: string | null;
        };
        /**
         * SessionOrigin
         * @description How a session came to exist.
         * @enum {string}
         */
        SessionOrigin: "imported" | "recorded" | "replay";
        /**
         * SessionResponse
         * @description Session response.
         */
        SessionResponse: {
            /**
             * Adapter Version
             * @description Recording adapter version.
             */
            adapter_version?: string | null;
            /**
             * Agent Id
             * Format: uuid
             * @description Agent the session belongs to.
             */
            agent_id: string;
            /**
             * Agent Version Id
             * @description Agent version recorded for the session.
             */
            agent_version_id?: string | null;
            /**
             * Cost
             * @description Total cost.
             */
            cost?: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Ended At
             * @description Time the session ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed session.
             */
            error?: string | null;
            /**
             * External Id
             * @description Id from the source system.
             */
            external_id?: string | null;
            /**
             * Framework
             * @description Agent framework used.
             */
            framework?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Session id.
             */
            id: string;
            /**
             * Import Id
             * @description Import the session was created by.
             */
            import_id?: string | null;
            /**
             * Imported From
             * @description Source system the session was imported from.
             */
            imported_from?: string | null;
            /**
             * Llm Call Count
             * @description Number of LLM call nodes.
             */
            llm_call_count: number;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Session name.
             */
            name?: string | null;
            /**
             * Number
             * @description Session number within the agent.
             */
            number: number;
            /** @description How the session came to exist. */
            origin: components["schemas"]["SessionOrigin"];
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Started At
             * @description Time the session started.
             */
            started_at?: string | null;
            status: components["schemas"]["SessionStatus"];
            /**
             * Task Id
             * @description Task the session was produced by.
             */
            task_id?: string | null;
            /** @description Total token usage. */
            tokens?: components["schemas"]["TokenUsage"] | null;
            /**
             * Tool Call Count
             * @description Number of tool call nodes.
             */
            tool_call_count: number;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * SessionRunCreateRequest
         * @description Session run create request.
         */
        SessionRunCreateRequest: {
            /**
             * Agent Version Id
             * Format: uuid
             * @description Agent version to run.
             */
            agent_version_id: string;
            /**
             * Inputs
             * @description Inputs passed to the agent's command.
             */
            inputs: unknown;
            /**
             * Name
             * @description Session name.
             */
            name?: string | null;
        };
        /**
         * SessionStatus
         * @description Session status.
         * @enum {string}
         */
        SessionStatus: "in_progress" | "completed" | "failed";
        /**
         * SessionUpdateRequest
         * @description Session update request.
         */
        SessionUpdateRequest: {
            /**
             * Ended At
             * @description New end time.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description New error.
             */
            error?: string | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
            /**
             * Name
             * @description New session name.
             */
            name?: string | null;
            /**
             * Output Text Selector
             * @description New output text selector.
             */
            output_text_selector?: string | null;
            /**
             * Outputs
             * @description New session outputs.
             */
            outputs?: unknown;
            /** @description New session status. */
            status?: components["schemas"]["SessionStatus"] | null;
        };
        /**
         * SessionWithEvaluationsResponse
         * @description Session with evaluations response.
         */
        SessionWithEvaluationsResponse: {
            /**
             * Evaluations
             * @description Every evaluation of the session, newest first.
             */
            evaluations: components["schemas"]["EvaluationResponse"][];
            /** @description Session. */
            session: components["schemas"]["SessionResponse"];
        };
        /**
         * SessionWithNodesResponse
         * @description Session with nodes response.
         */
        SessionWithNodesResponse: {
            /**
             * Nodes
             * @description Every node of the session, ordered by index ascending.
             */
            nodes: components["schemas"]["SessionNodeResponse"][];
            /** @description Session. */
            session: components["schemas"]["SessionDetailResponse"];
        };
        /**
         * SetupCommandHook
         * @description Setup command hook.
         */
        SetupCommandHook: {
            /**
             * Command
             * @description Shell command to run.
             */
            command: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "setup_command";
        };
        /**
         * StaticCase
         * @description Static tool call case.
         */
        StaticCase: {
            /**
             * Match
             * @description Call arguments to match, unset matches any call.
             */
            match?: unknown | null;
            /** @description How the call arguments are matched. */
            match_mode: components["schemas"]["StaticMatchMode"];
            /**
             * Result
             * @description Result returned for a matching call.
             */
            result: unknown;
        };
        /**
         * StaticConfig
         * @description Static tool config.
         */
        StaticConfig: {
            /**
             * Cases
             * @description Cases tried in order for a matching call.
             */
            cases: components["schemas"]["StaticCase"][];
            /** @description Behavior when no case matches. */
            on_miss: components["schemas"]["ToolPolicyOnMiss"];
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "static";
        };
        /**
         * StaticMatchMode
         * @description How a static case matches a tool call.
         * @enum {string}
         */
        StaticMatchMode: "exact" | "subset";
        /**
         * TagCreateRequest
         * @description Tag create request.
         */
        TagCreateRequest: {
            /**
             * Name
             * @description Tag name.
             */
            name: string;
        };
        /**
         * TagLinkCreateRequest
         * @description Tag link create request.
         */
        TagLinkCreateRequest: {
            /**
             * Resource Id
             * Format: uuid
             * @description Resource being tagged.
             */
            resource_id: string;
            /** @description Kind of resource being tagged. */
            resource_type: components["schemas"]["TagResourceType"];
        };
        /**
         * TagLinkResponse
         * @description Tag link response.
         */
        TagLinkResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Tag link id.
             */
            id: string;
            /**
             * Resource Id
             * Format: uuid
             * @description Resource tagged.
             */
            resource_id: string;
            /** @description Kind of resource tagged. */
            resource_type: components["schemas"]["TagResourceType"];
            /**
             * Tag Id
             * Format: uuid
             * @description Tag applied.
             */
            tag_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * TagResourceType
         * @description Resource kind a tag link points at.
         * @enum {string}
         */
        TagResourceType: "session" | "cohort" | "cohort_version" | "agent_version" | "experiment" | "experiment_run";
        /**
         * TagResponse
         * @description Tag response.
         */
        TagResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Tag id.
             */
            id: string;
            /**
             * Name
             * @description Tag name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * TagUpdateRequest
         * @description Tag update request.
         */
        TagUpdateRequest: {
            /**
             * Name
             * @description New tag name.
             */
            name: string;
        };
        /**
         * TaskClaimRequest
         * @description Task claim request.
         */
        TaskClaimRequest: {
            /**
             * Max Tasks
             * @description Maximum number of tasks to claim.
             */
            max_tasks: number;
        };
        /**
         * TaskClaimResponse
         * @description Task claim response.
         */
        TaskClaimResponse: {
            /**
             * Tasks
             * @description Claimed tasks.
             */
            tasks: components["schemas"]["TaskWithSpec"][];
        };
        /**
         * TaskKind
         * @description Kind of work a task runs.
         * @enum {string}
         */
        TaskKind: "agent" | "evaluator" | "importer";
        /**
         * TaskOnFailure
         * @description What a task's hard failure does to the rest of its job.
         * @enum {string}
         */
        TaskOnFailure: "abort" | "continue" | "ignore";
        /**
         * TaskResponse
         * @description Task response.
         */
        TaskResponse: {
            /**
             * Agent Version Id
             * @description Agent version run by an agent task.
             */
            agent_version_id?: string | null;
            /**
             * Attempt
             * @description Current attempt number.
             */
            attempt: number;
            /**
             * Cancel Requested At
             * @description Time cancellation was requested.
             */
            cancel_requested_at?: string | null;
            /**
             * Claimed At
             * @description Time the task was claimed.
             */
            claimed_at?: string | null;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Ended At
             * @description Time execution ended.
             */
            ended_at?: string | null;
            /**
             * Error
             * @description Error from a failed task.
             */
            error?: string | null;
            /**
             * Heartbeat At
             * @description Time of the worker's last heartbeat.
             */
            heartbeat_at?: string | null;
            /**
             * Id
             * Format: uuid
             * @description Task id.
             */
            id: string;
            /**
             * Import Id
             * @description Import run by an importer task.
             */
            import_id?: string | null;
            /**
             * Input Session Id
             * @description Input session for an evaluator task.
             */
            input_session_id?: string | null;
            /**
             * Job Id
             * Format: uuid
             * @description Owning job.
             */
            job_id: string;
            /** @description Kind of work the task runs. */
            kind: components["schemas"]["TaskKind"];
            /**
             * Labels
             * @description Labels matched by worker scope selectors.
             */
            labels: {
                [key: string]: string;
            };
            /** @description Effect of a hard failure on the job. */
            on_failure: components["schemas"]["TaskOnFailure"];
            /**
             * Plugin Version Id
             * @description Plugin version run by an evaluator task.
             */
            plugin_version_id?: string | null;
            /**
             * Result
             * @description Task result, diagnostic output on a non-completed task.
             */
            result: unknown;
            /**
             * Started At
             * @description Time execution started.
             */
            started_at?: string | null;
            status: components["schemas"]["TaskStatus"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
            /**
             * Worker Id
             * @description Worker that claimed the task.
             */
            worker_id?: string | null;
        };
        /**
         * TaskRunSpec
         * @description Task run spec.
         */
        TaskRunSpec: {
            /**
             * Command
             * @description Shell command to run.
             */
            command: string;
            /**
             * Env
             * @description Process environment from the run spec.
             */
            env: {
                [key: string]: string;
            };
            /**
             * Working Dir
             * @description Working directory.
             */
            working_dir?: string | null;
        };
        /**
         * TaskSpecResponse
         * @description Task spec response.
         */
        TaskSpecResponse: {
            /**
             * Details
             * @description Kind-specific task details.
             */
            details: components["schemas"]["AgentTaskDetails"] | components["schemas"]["EvaluationTaskDetails"] | components["schemas"]["ImportTaskDetails"];
            /**
             * Env
             * @description Creator-set process environment extras.
             */
            env: {
                [key: string]: string;
            };
            /**
             * Hooks
             * @description Hooks run around the task process.
             */
            hooks?: (components["schemas"]["CopyWorkdirHook"] | components["schemas"]["SetupCommandHook"] | components["schemas"]["TeardownCommandHook"])[];
            /** @description Kind of work the task runs. */
            kind: components["schemas"]["TaskKind"];
            /** @description Command to run, unset for evaluator and importer tasks. */
            run?: components["schemas"]["TaskRunSpec"] | null;
            /**
             * Secret Env
             * @description Secrets merged into the process environment.
             */
            secret_env: {
                [key: string]: string;
            };
            /**
             * Task Id
             * Format: uuid
             * @description Task the spec belongs to.
             */
            task_id: string;
            /**
             * Timeout Seconds
             * @description Process timeout.
             */
            timeout_seconds: number;
        };
        /**
         * TaskStatus
         * @description Task status.
         * @enum {string}
         */
        TaskStatus: "pending" | "claimed" | "running" | "completed" | "failed" | "timed_out" | "canceled" | "abandoned";
        /**
         * TaskUpdateRequest
         * @description Task update request.
         */
        TaskUpdateRequest: {
            /**
             * Error
             * @description New error.
             */
            error?: string | null;
            /**
             * Result
             * @description New task result.
             */
            result?: unknown | null;
            /** @description New task status. */
            status?: components["schemas"]["TaskStatus"] | null;
        };
        /**
         * TaskWithSpec
         * @description Task with spec.
         */
        TaskWithSpec: {
            /** @description Task spec. */
            spec: components["schemas"]["TaskSpecResponse"];
            /** @description Task. */
            task: components["schemas"]["TaskResponse"];
            /**
             * Token
             * @description Bearer token scoped to this task and attempt.
             */
            token: string;
        };
        /**
         * TeardownCommandHook
         * @description Teardown command hook.
         */
        TeardownCommandHook: {
            /**
             * Command
             * @description Shell command to run.
             */
            command: string;
            /**
             * On
             * @description Task process outcome the command runs on.
             * @default success
             * @enum {string}
             */
            on: "success" | "failure" | "always";
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "teardown_command";
        };
        /**
         * TextInsightData
         * @description Text insight data.
         */
        TextInsightData: {
            /**
             * Content
             * @description Markdown content.
             */
            content: string;
            /**
             * @description discriminator enum property added by openapi-typescript
             * @enum {string}
             */
            type: "text";
        };
        /**
         * TokenErrorCode
         * @description Device authorization grant error code.
         * @enum {string}
         */
        TokenErrorCode: "authorization_pending" | "slow_down" | "access_denied" | "expired_token" | "invalid_grant" | "invalid_request" | "unsupported_grant_type";
        /**
         * TokenErrorResponse
         * @description Token error response.
         */
        TokenErrorResponse: {
            /**
             * Detail
             * @description Error message.
             */
            detail: string;
            /** @description Error code. */
            error: components["schemas"]["TokenErrorCode"];
        };
        /**
         * TokenResponse
         * @description Token response.
         */
        TokenResponse: {
            /**
             * Access Token
             * @description Bearer token.
             */
            access_token: string;
            /**
             * Csrf Token
             * @description CSRF token for cookie authentication.
             */
            csrf_token?: string | null;
            /**
             * Expires In
             * @description Token lifetime in seconds.
             */
            expires_in: number;
            /**
             * Token Type
             * @description Token type.
             */
            token_type: string;
        };
        /**
         * TokenUsage
         * @description Token usage.
         */
        TokenUsage: {
            /**
             * Cached Input Tokens
             * @description Cached input tokens.
             */
            cached_input_tokens?: number | null;
            /**
             * Input Tokens
             * @description Input tokens.
             */
            input_tokens?: number | null;
            /**
             * Output Tokens
             * @description Output tokens.
             */
            output_tokens?: number | null;
            /**
             * Reasoning Tokens
             * @description Reasoning tokens.
             */
            reasoning_tokens?: number | null;
        };
        /**
         * ToolLookupMatch
         * @description Tool lookup match.
         */
        ToolLookupMatch: {
            /**
             * Error
             * @description Error from a failed tool call.
             */
            error?: string | null;
            /**
             * Result
             * @description Cached tool result.
             */
            result: unknown;
            /** @description Tool call status. */
            status: components["schemas"]["NodeStatus"];
        };
        /**
         * ToolLookupRequest
         * @description Tool lookup request.
         */
        ToolLookupRequest: {
            /**
             * Cache Key
             * @description Call cache key.
             */
            cache_key: string;
            /**
             * Occurrence
             * @description Zero-based match position in baseline order, the newest match when unset.
             */
            occurrence?: number | null;
            /**
             * Tool Name
             * @description Tool being called.
             */
            tool_name: string;
        };
        /**
         * ToolLookupResponse
         * @description Tool lookup response.
         */
        ToolLookupResponse: {
            /** @description Matching recorded tool call. */
            match?: components["schemas"]["ToolLookupMatch"] | null;
        };
        /**
         * ToolPolicy
         * @description Tool policy.
         */
        ToolPolicy: {
            /**
             * Default
             * @description Config applied to tools without an override.
             */
            default: components["schemas"]["PassthroughConfig"] | components["schemas"]["HistoryConfig"] | components["schemas"]["StaticConfig"] | components["schemas"]["LLMConfig"];
            /**
             * Tools
             * @description Per-tool config overrides.
             */
            tools?: {
                [key: string]: components["schemas"]["PassthroughConfig"] | components["schemas"]["HistoryConfig"] | components["schemas"]["StaticConfig"] | components["schemas"]["LLMConfig"];
            };
        };
        /**
         * ToolPolicyOnMiss
         * @description Behavior when a replayed tool call has no match.
         * @enum {string}
         */
        ToolPolicyOnMiss: "fail" | "passthrough" | "error_result";
        /**
         * UserActivateRequest
         * @description User activate request.
         */
        UserActivateRequest: {
            /**
             * Activation Token
             * @description Activation token.
             */
            activation_token: string;
            /**
             * Password
             * @description Login password to set.
             */
            password: string;
        };
        /**
         * UserActivationTokenResponse
         * @description User response carrying a newly minted activation token.
         */
        UserActivationTokenResponse: {
            /**
             * Activation Token
             * @description Plaintext token, shown once.
             */
            activation_token: string;
            /**
             * Active
             * @description Whether the account can authenticate.
             */
            active: boolean;
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Email
             * @description Contact email.
             */
            email: string | null;
            /**
             * Id
             * Format: uuid
             * @description Account id.
             */
            id: string;
            /**
             * Is Admin
             * @description Whether the account has admin rights.
             */
            is_admin: boolean;
            /**
             * Is Service Account
             * @description Whether this is a service account.
             */
            is_service_account: boolean;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: unknown;
            };
            /**
             * Name
             * @description Account name.
             */
            name: string;
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * UserCreateRequest
         * @description User create request.
         */
        UserCreateRequest: {
            /**
             * Email
             * @description Contact email.
             */
            email?: string | null;
            /**
             * Is Admin
             * @description Whether the account has admin rights.
             * @default false
             */
            is_admin: boolean;
            /**
             * Name
             * @description Account name.
             */
            name: string;
            /**
             * Password
             * @description Login password.
             */
            password?: string | null;
        };
        /**
         * UserUpdateRequest
         * @description User update request.
         */
        UserUpdateRequest: {
            /**
             * Is Admin
             * @description New admin rights state.
             */
            is_admin?: boolean | null;
            /**
             * Metadata
             * @description New metadata.
             */
            metadata?: {
                [key: string]: unknown;
            } | null;
            /**
             * Old Password
             * @description Current login password.
             */
            old_password?: string | null;
            /**
             * Password
             * @description New login password.
             */
            password?: string | null;
        };
        /**
         * ValidationErrorBody
         * @description Validation error body.
         */
        ValidationErrorBody: {
            /**
             * Detail
             * @description Error detail.
             */
            detail: string | components["schemas"]["ValidationErrorItem"][];
        };
        /**
         * ValidationErrorItem
         * @description Validation error item.
         */
        ValidationErrorItem: {
            /**
             * Ctx
             * @description Error context.
             */
            ctx?: {
                [key: string]: unknown;
            };
            /**
             * Input
             * @description Invalid input value.
             */
            input?: unknown;
            /**
             * Loc
             * @description Path to the invalid input.
             */
            loc: (string | number)[];
            /**
             * Msg
             * @description Error message.
             */
            msg: string;
            /**
             * Type
             * @description Error type identifier.
             */
            type: string;
        };
        /**
         * WorkerClaim
         * @description Worker claim.
         */
        WorkerClaim: {
            /**
             * Agent Version Id
             * @description Agent version the claim covers, None claims every agent version.
             */
            agent_version_id?: string | null;
            /** @description Task kind the claim covers. */
            kind: components["schemas"]["TaskKind"];
        };
        /**
         * WorkerCreateRequest
         * @description Worker create request.
         */
        WorkerCreateRequest: {
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata?: {
                [key: string]: string;
            };
            /**
             * Name
             * @description Worker name.
             */
            name: string;
            /** @description Runtime the worker reports. */
            runtime: components["schemas"]["WorkerRuntime"];
            /** @description Tasks this worker is willing to claim. */
            scope: components["schemas"]["WorkerScope"];
        };
        /**
         * WorkerHeartbeatRequest
         * @description Worker heartbeat request.
         */
        WorkerHeartbeatRequest: {
            /**
             * Task Ids
             * @description Tasks the worker currently holds.
             */
            task_ids: string[];
        };
        /**
         * WorkerHeartbeatResponse
         * @description Worker heartbeat response.
         */
        WorkerHeartbeatResponse: {
            /**
             * Cancel Task Ids
             * @description Held tasks whose cancellation was requested.
             */
            cancel_task_ids: string[];
        };
        /**
         * WorkerRegistrationResponse
         * @description Worker registration response.
         */
        WorkerRegistrationResponse: {
            /**
             * Token
             * @description Bearer token scoped to this worker.
             */
            token: string;
            /**
             * Token Expires At
             * Format: date-time
             * @description Time the token expires.
             */
            token_expires_at: string;
            /** @description Registered worker. */
            worker: components["schemas"]["WorkerResponse"];
        };
        /**
         * WorkerResponse
         * @description Worker response.
         */
        WorkerResponse: {
            /**
             * Created
             * Format: date-time
             * @description Creation time.
             */
            created: string;
            /**
             * Id
             * Format: uuid
             * @description Worker id.
             */
            id: string;
            /**
             * Last Seen At
             * Format: date-time
             * @description Time of the worker's last heartbeat.
             */
            last_seen_at: string;
            /**
             * Live
             * @description Whether the worker is considered alive.
             */
            live: boolean;
            /**
             * Metadata
             * @description Arbitrary metadata.
             */
            metadata: {
                [key: string]: string;
            };
            /**
             * Name
             * @description Worker name.
             */
            name: string;
            /**
             * Owner Id
             * Format: uuid
             * @description Id of the owning account.
             */
            owner_id: string;
            /** @description Runtime the worker reports. */
            runtime: components["schemas"]["WorkerRuntime"];
            /** @description Tasks this worker is willing to claim. */
            scope: components["schemas"]["WorkerScope"];
            /**
             * Updated
             * Format: date-time
             * @description Last modification time.
             */
            updated: string;
        };
        /**
         * WorkerRuntime
         * @description Worker runtime.
         */
        WorkerRuntime: {
            /**
             * Arch
             * @description Reported architecture.
             */
            arch?: string | null;
            /**
             * Hostname
             * @description Reported hostname.
             */
            hostname?: string | null;
            /**
             * Kitaru Version
             * @description Reported Kitaru version.
             */
            kitaru_version?: string | null;
            /**
             * Namespace
             * @description Reported Kubernetes namespace.
             */
            namespace?: string | null;
            /**
             * Os
             * @description Reported operating system.
             */
            os?: string | null;
            /**
             * Platform
             * @description Runtime platform, e.g. kubernetes, docker, bare.
             */
            platform: string;
            /**
             * Pod
             * @description Reported Kubernetes pod name.
             */
            pod?: string | null;
            /**
             * Python Version
             * @description Reported Python version.
             */
            python_version?: string | null;
        };
        /**
         * WorkerScope
         * @description Worker scope.
         */
        WorkerScope: {
            /**
             * Claims
             * @description Claims the worker serves, combined by disjunction.
             */
            claims: components["schemas"]["WorkerClaim"][];
            /**
             * Job Id
             * @description Job the worker claims tasks from.
             */
            job_id?: string | null;
            /**
             * Selectors
             * @description Label selectors the worker claims, combined by conjunction.
             */
            selectors?: components["schemas"]["LabelSelector"][] | null;
        };
        /**
         * WorkerTokenResponse
         * @description Worker token response.
         */
        WorkerTokenResponse: {
            /**
             * Token
             * @description Bearer token scoped to this worker.
             */
            token: string;
            /**
             * Token Expires At
             * Format: date-time
             * @description Time the token expires.
             */
            token_expires_at: string;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    list_accounts_api_v1_accounts_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_AccountResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_current_account_api_v1_accounts_me_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_account_api_v1_accounts__account_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                account_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_agent_version_api_v1_agent_versions__agent_version_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_version_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_agent_version_api_v1_agent_versions__agent_version_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_version_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_agent_version_api_v1_agent_versions__agent_version_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_version_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AgentVersionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_agents_api_v1_agents_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_AgentResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_agent_api_v1_agents_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AgentCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_agent_api_v1_agents__agent_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_agent_api_v1_agents__agent_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_agent_api_v1_agents__agent_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                agent_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AgentUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_agent_versions_api_v1_agents__agent_id__versions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path: {
                agent_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_AgentVersionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_agent_version_api_v1_agents__agent_id__versions_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                agent_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AgentVersionCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AgentVersionResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_annotations_api_v1_annotations_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_AnnotationResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_annotation_api_v1_annotations_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ManualAnnotationCreateRequest"] | components["schemas"]["InvestigationAnswerCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AnnotationResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_annotation_api_v1_annotations__annotation_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                annotation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AnnotationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_annotation_api_v1_annotations__annotation_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                annotation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_annotation_api_v1_annotations__annotation_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                annotation_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AnnotationUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AnnotationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_api_keys_api_v1_api_keys_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ApiKeyResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_api_key_api_v1_api_keys_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ApiKeyCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiKeyIssuedResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_api_key_api_v1_api_keys__api_key_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                api_key_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiKeyResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_api_key_api_v1_api_keys__api_key_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                api_key_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_api_key_api_v1_api_keys__api_key_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                api_key_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ApiKeyUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiKeyResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    rotate_api_key_api_v1_api_keys__api_key_id__rotate_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                api_key_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ApiKeyRotateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiKeyIssuedResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    upload_blob_api_v1_blobs_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "multipart/form-data": components["schemas"]["Body_upload_blob_api_v1_blobs_post"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["BlobResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Content Too Large */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_blob_api_v1_blobs__blob_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                blob_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["BlobResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_blob_api_v1_blobs__blob_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                blob_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    download_blob_api_v1_blobs__blob_id__content_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                blob_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_cohort_version_api_v1_cohort_versions__cohort_version_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_version_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_cohort_version_api_v1_cohort_versions__cohort_version_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_version_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_cohort_version_api_v1_cohort_versions__cohort_version_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_version_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["CohortVersionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_cohorts_api_v1_cohorts_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_CohortResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_cohort_api_v1_cohorts_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["CohortCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_cohort_api_v1_cohorts__cohort_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_cohort_api_v1_cohorts__cohort_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_cohort_api_v1_cohorts__cohort_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                cohort_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["CohortUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_cohort_versions_api_v1_cohorts__cohort_id__versions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path: {
                cohort_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_CohortVersionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_cohort_version_api_v1_cohorts__cohort_id__versions_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                cohort_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["CohortVersionCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["CohortVersionResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    device_authorization_api_v1_device_authorization_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: {
            content: {
                "application/x-www-form-urlencoded": components["schemas"]["Body_device_authorization_api_v1_device_authorization_post"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DeviceAuthorizationResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TokenErrorResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_devices_api_v1_devices_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_DeviceResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_device_api_v1_devices__device_id__get: {
        parameters: {
            query?: {
                user_code?: string | null;
            };
            header?: never;
            path: {
                device_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DeviceResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_device_api_v1_devices__device_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                device_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_device_api_v1_devices__device_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                device_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["DeviceUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DeviceResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    verify_device_api_v1_devices__device_id__verify_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                device_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["DeviceVerifyRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["DeviceResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_evaluations_api_v1_evaluations_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_EvaluationResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_evaluations_api_v1_evaluations_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EvaluationBatchCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_evaluation_api_v1_evaluations__evaluation_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_evaluators_api_v1_evaluators_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_EvaluatorResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_evaluator_api_v1_evaluators_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EvaluatorCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_evaluator_api_v1_evaluators__evaluator_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluator_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_evaluator_api_v1_evaluators__evaluator_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluator_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_evaluator_api_v1_evaluators__evaluator_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluator_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EvaluatorUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_evaluator_versions_api_v1_evaluators__evaluator_id__versions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
            };
            header?: never;
            path: {
                evaluator_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_EvaluatorVersionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_evaluator_version_api_v1_evaluators__evaluator_id__versions_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                evaluator_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EvaluatorVersionCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorVersionResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_evaluator_version_api_v1_evaluators__evaluator_id__versions__version__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluator_id: string;
                version: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_evaluator_version_api_v1_evaluators__evaluator_id__versions__version__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                evaluator_id: string;
                version: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["EvaluatorVersionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluatorVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_experiment_runs_api_v1_experiment_runs_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ExperimentRunResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_experiment_run_api_v1_experiment_runs__experiment_run_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentRunResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_experiment_run_api_v1_experiment_runs__experiment_run_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    cancel_experiment_run_api_v1_experiment_runs__experiment_run_id__cancel_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentRunResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_experiment_run_jobs_api_v1_experiment_runs__experiment_run_id__jobs_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path: {
                experiment_run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_JobResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_experiments_api_v1_experiments_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ExperimentResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_experiment_api_v1_experiments_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ExperimentCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_experiment_api_v1_experiments__experiment_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_experiment_api_v1_experiments__experiment_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_experiment_api_v1_experiments__experiment_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ExperimentUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    start_run_api_v1_experiments__experiment_id__runs_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                experiment_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ExperimentRunCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ExperimentRunResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_importers_api_v1_importers_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ImporterResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_importer_api_v1_importers_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ImporterCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_importer_api_v1_importers__importer_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                importer_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_importer_api_v1_importers__importer_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                importer_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_importer_api_v1_importers__importer_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                importer_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ImporterUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_importer_versions_api_v1_importers__importer_id__versions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
            };
            header?: never;
            path: {
                importer_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ImporterVersionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_importer_version_api_v1_importers__importer_id__versions_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path: {
                importer_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ImporterVersionCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterVersionResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_importer_version_api_v1_importers__importer_id__versions__version__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                importer_id: string;
                version: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_importer_version_api_v1_importers__importer_id__versions__version__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                importer_id: string;
                version: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ImporterVersionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImporterVersionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_imports_api_v1_imports_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ImportResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_import_api_v1_imports_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ImportCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImportResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_import_api_v1_imports__import_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                import_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ImportResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_info_api_v1_info_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ServerInfoResponse"];
                };
            };
        };
    };
    list_insights_api_v1_insights_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_InsightResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_insights_api_v1_insights_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InsightBatchCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InsightResponse"][];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_insight_api_v1_insights__insight_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                insight_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InsightResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_insight_api_v1_insights__insight_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                insight_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_insight_api_v1_insights__insight_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                insight_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InsightUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InsightResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_investigations_api_v1_investigations_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_InvestigationResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_investigation_api_v1_investigations_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InvestigationCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InvestigationResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_investigation_api_v1_investigations__investigation_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                investigation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InvestigationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_investigation_api_v1_investigations__investigation_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                investigation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_investigation_api_v1_investigations__investigation_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                investigation_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InvestigationUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InvestigationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_investigation_sessions_api_v1_investigations__investigation_id__sessions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
            };
            header?: never;
            path: {
                investigation_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_InvestigationSessionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_investigation_session_verdict_api_v1_investigations__investigation_id__sessions__session_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                investigation_id: string;
                session_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["InvestigationSessionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["InvestigationSessionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_jobs_api_v1_jobs_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_JobResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_job_api_v1_jobs__job_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_job_api_v1_jobs__job_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    cancel_job_api_v1_jobs__job_id__cancel_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_job_tasks_api_v1_jobs__job_id__tasks_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path: {
                job_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_TaskResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    login_api_v1_login_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: {
            content: {
                "application/x-www-form-urlencoded": components["schemas"]["Body_login_api_v1_login_post"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TokenResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TokenErrorResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    logout_api_v1_logout_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
        };
    };
    list_replays_api_v1_replays_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_ReplayResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_replay_api_v1_replays_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ReplayCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ReplayResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_replay_api_v1_replays__replay_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                replay_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ReplayResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_replay_api_v1_replays__replay_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                replay_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    tool_lookup_api_v1_replays__replay_id__tool_lookup_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                replay_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ToolLookupRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ToolLookupResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_secrets_api_v1_secrets_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_SecretResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_secret_api_v1_secrets_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SecretCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SecretResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_secret_api_v1_secrets__secret_id__get: {
        parameters: {
            query?: {
                include_values?: boolean;
            };
            header?: never;
            path: {
                secret_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SecretResponse"] | components["schemas"]["SecretWithValuesResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_secret_api_v1_secrets__secret_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                secret_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_secret_api_v1_secrets__secret_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                secret_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SecretUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SecretResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_service_account_api_v1_service_accounts_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ServiceAccountCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_service_account_api_v1_service_accounts__account_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                account_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ServiceAccountUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_session_run_api_v1_session_runs_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SessionRunCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["JobResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_sessions_api_v1_sessions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
                /** @description Include inputs and outputs. */
                include_payloads?: boolean;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_SessionDetailResponse_"] | components["schemas"]["Page_SessionResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_session_api_v1_sessions_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SessionCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_session_api_v1_sessions__session_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionDetailResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_session_api_v1_sessions__session_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_session_api_v1_sessions__session_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SessionUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_session_evaluations_api_v1_sessions__session_id__evaluations_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SessionEvaluationsRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluationResponse"][];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_session_with_nodes_api_v1_sessions__session_id__full_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionWithNodesResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_session_nodes_api_v1_sessions__session_id__nodes_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Include reasoning, inputs, outputs, and attributes. */
                include_payloads?: boolean;
            };
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_SessionNodeResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    ingest_session_nodes_api_v1_sessions__session_id__nodes_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["SessionNodeBatchRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionNodeResponse"][];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_tags_api_v1_tags_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_TagResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_tag_api_v1_tags_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TagCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TagResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_tag_api_v1_tags__tag_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                tag_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_tag_api_v1_tags__tag_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                tag_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TagUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TagResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_tag_link_api_v1_tags__tag_id__links_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                tag_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TagLinkCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TagLinkResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_tag_link_api_v1_tags__tag_id__links__resource_type___resource_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                tag_id: string;
                resource_type: components["schemas"]["TagResourceType"];
                resource_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_tasks_api_v1_tasks_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_TaskResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    claim_tasks_api_v1_tasks_claim_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TaskClaimRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TaskClaimResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_task_api_v1_tasks__task_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                task_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TaskResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_task_api_v1_tasks__task_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                task_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TaskUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TaskResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Content Too Large */
            413: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_task_spec_api_v1_tasks__task_id__spec_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                task_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["TaskSpecResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_experiment_run_evaluation_aggregates_api_v1_ui_experiment_runs__experiment_run_id__evaluation_aggregates_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                experiment_run_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["EvaluationAggregateResponse"][];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_sample_data_api_v1_ui_sample_data_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: {
            content: {
                "application/json": components["schemas"]["SampleDataCreateRequest"] | null;
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SampleDataResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_sessions_with_evaluations_api_v1_ui_sessions_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
                /** @description Include inputs and outputs. */
                include_payloads?: boolean;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_SessionWithEvaluationsResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_session_with_evaluations_api_v1_ui_sessions__session_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                session_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["SessionDetailWithEvaluationsResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    create_user_api_v1_users_post: {
        parameters: {
            query?: never;
            header?: {
                "Idempotency-Key"?: string | null;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["UserCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            201: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"] | components["schemas"]["UserActivationTokenResponse"];
                };
            };
            /** @description Bad Request */
            400: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Conflict */
            409: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    update_user_api_v1_users__account_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                account_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["UserUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    activate_user_api_v1_users__account_id__activate_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                account_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["UserActivateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["AccountResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    deactivate_user_api_v1_users__account_id__deactivate_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                account_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["UserActivationTokenResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    list_workers_api_v1_workers_get: {
        parameters: {
            query?: {
                /** @description Cursor from the previous page. */
                cursor?: string | null;
                /** @description Items per page. */
                size?: number;
                /** @description Sort field and direction, as field:asc or field:desc. */
                sort?: string;
                /** @description Filter expression, JSON-encoded in the query string. */
                filter?: components["schemas"]["FilterCondition"] | components["schemas"]["AndFilter"] | components["schemas"]["OrFilter"] | components["schemas"]["NotFilter"] | null;
                /** @description Include workers past the liveness window. */
                include_stale?: boolean;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["Page_WorkerResponse_"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    register_worker_api_v1_workers_post: {
        parameters: {
            query?: never;
            header?: {
                "X-Kitaru-Client"?: string;
            };
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["WorkerCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkerRegistrationResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Upgrade Required */
            426: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    get_worker_api_v1_workers__worker_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                worker_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkerResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    delete_worker_api_v1_workers__worker_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                worker_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            204: {
                headers: {
                    [name: string]: unknown;
                };
                content?: never;
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    heartbeat_worker_api_v1_workers__worker_id__heartbeat_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                worker_id: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["WorkerHeartbeatRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkerHeartbeatResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    renew_worker_token_api_v1_workers__worker_id__token_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                worker_id: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["WorkerTokenResponse"];
                };
            };
            /** @description Unauthorized */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Forbidden */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Not Found */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ValidationErrorBody"];
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    health_health_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": {
                        [key: string]: string;
                    };
                };
            };
            /** @description Service Unavailable */
            503: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ErrorBody"];
                };
            };
        };
    };
    liveness_health_live_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": {
                        [key: string]: string;
                    };
                };
            };
        };
    };
}
