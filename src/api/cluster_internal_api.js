/* Copyright (C) 2016 NooBaa */
'use strict';

/**
 *
 * CLUSTER INTERNAL API
 *
 * Cluster & HA
 *
 */
module.exports = {

    $id: 'cluster_internal_api',

    methods: {
        verify_join_conditions: {
            doc: 'check join conditions to the cluster and return caller ip (stun)',
            method: 'GET',
            params: {
                type: 'object',
                required: ['secret'],
                properties: {
                    secret: {
                        type: 'string'
                    }
                }
            },
            reply: {
                type: 'object',
                required: ['caller_address', 'hostname', 'result'],
                properties: {
                    caller_address: {
                        type: 'string'
                    },
                    hostname: {
                        type: 'string'
                    },
                    result: {
                        $ref: 'cluster_server_api#/definitions/verify_new_member_result'
                    }
                }
            },
            auth: {
                system: false
            }
        },

        get_secret: {
            doc: 'get server secret',
            method: 'GET',
            reply: {
                type: 'object',
                required: ['secret'],
                properties: {
                    secret: {
                        type: 'string'
                    },
                }
            },
            auth: {
                system: false
            }
        },

        get_version: {
            doc: 'get server version',
            method: 'GET',
            reply: {
                type: 'object',
                required: ['version'],
                properties: {
                    version: {
                        type: 'string'
                    },
                }
            },
            auth: {
                system: false
            }
        },

        apply_set_debug_level: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['level'],
                properties: {
                    target_secret: {
                        type: 'string',
                    },
                    level: {
                        type: 'integer',
                    }
                },
            },
            auth: {
                system: 'admin',
            }
        },

        collect_server_diagnostics: {
            method: 'POST',
            reply: {
                type: 'object',
                properties: {
                    // [RPC_BUFFERS].data
                },
            },
            auth: {
                system: 'admin',
            }
        },

        apply_read_server_time: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['target_secret'],
                properties: {
                    target_secret: {
                        type: 'string',
                    }
                },
            },
            reply: {
                idate: true,
            },
            auth: {
                system: false,
            }
        },

        set_hostname_internal: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['hostname'],
                properties: {
                    hostname: {
                        type: 'string'
                    }
                }
            },
            auth: {
                system: false,
            }
        },

    },

    definitions: {
        time_config: {
            type: 'object',
            required: ['timezone'],
            properties: {
                target_secret: {
                    type: 'string'
                },
                timezone: {
                    type: 'string'
                },
                epoch: {
                    type: 'number'
                },
            },
        },
    },
};
