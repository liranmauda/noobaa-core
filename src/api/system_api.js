'use strict';

/**
 *
 * SYSTEM API
 *
 * client (currently web client) talking to the web server to work on system
 * (per client group - contains nodes, tiers abd bckets etc)
 *
 */
module.exports = {

    id: 'system_api',

    methods: {

        create_system: {
            doc: 'Create a new system',
            method: 'POST',
            params: {
                type: 'object',
                required: ['name', 'email', 'password', 'activation_code'],
                properties: {
                    name: {
                        type: 'string',
                    },
                    email: {
                        type: 'string',
                    },
                    password: {
                        type: 'string',
                    },
                    activation_code: {
                        type: 'string',
                    },
                    //Optionals: DNS, NTP and NooBaa Domain Name
                    time_config: {
                        $ref: 'cluster_internal_api#/definitions/time_config'
                    },
                    dns_servers: {
                        type: 'array',
                        items: {
                            type: 'string'
                        },
                    },
                    dns_name: {
                        type: 'string'
                    }
                },
            },
            reply: {
                type: 'object',
                required: ['token'],
                properties: {
                    token: {
                        type: 'string'
                    }
                }
            },
            auth: {
                account: false,
                system: false,
            }
        },

        read_system: {
            doc: 'Read the info of the authorized system',
            method: 'GET',
            reply: {
                $ref: '#/definitions/system_full_info'
            },
            auth: {
                system: 'admin',
            }
        },

        update_system: {
            doc: 'Update the authorized system',
            method: 'PUT',
            params: {
                type: 'object',
                required: ['name'],
                properties: {
                    name: {
                        type: 'string',
                    },
                },
            },
            auth: {
                system: 'admin',
            }
        },

        set_maintenance_mode: {
            doc: 'Configure system maintenance',
            method: 'PUT',
            params: {
                type: 'object',
                required: ['duration'],
                properties: {
                    // Number of minutes
                    duration: {
                        type: 'number',
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        set_webserver_master_state: {
            doc: 'Set if webserver is master',
            method: 'PUT',
            params: {
                type: 'object',
                required: ['is_master'],
                properties: {
                    is_master: {
                        type: 'boolean',
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        delete_system: {
            doc: 'Delete the authorized system',
            method: 'DELETE',
            auth: {
                system: 'admin',
            }
        },


        list_systems: {
            doc: 'List the systems that the authorized account can access',
            method: 'GET',
            reply: {
                type: 'object',
                required: ['systems'],
                properties: {
                    systems: {
                        type: 'array',
                        items: {
                            $ref: '#/definitions/system_info'
                        }
                    }
                }
            },
            auth: {
                system: false,
            }
        },

        log_frontend_stack_trace: {
            doc: 'Add frontend stack trace to logs',
            method: 'POST',
            params: {
                type: 'object',
                required: ['stack_trace'],
                properties: {
                    stack_trace: {
                        type: 'object',
                        additionalProperties: true,
                        properties: {},
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        add_role: {
            doc: 'Add role',
            method: 'POST',
            params: {
                type: 'object',
                required: ['role', 'email'],
                properties: {
                    email: {
                        type: 'string',
                    },
                    role: {
                        $ref: '#/definitions/role_enum'
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        remove_role: {
            doc: 'Remove role',
            method: 'DELETE',
            params: {
                type: 'object',
                required: ['role', 'email'],
                properties: {
                    email: {
                        type: 'string',
                    },
                    role: {
                        $ref: '#/definitions/role_enum'
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        set_last_stats_report_time: {
            doc: 'Set last stats report sync time',
            method: 'PUT',
            params: {
                type: 'object',
                required: ['last_stats_report'],
                properties: {
                    last_stats_report: {
                        format: 'idate',
                    },
                }
            },
            auth: {
                system: 'admin',
            }
        },

        diagnose_system: {
            method: 'GET',
            reply: {
                type: 'string',
            },
            auth: {
                system: 'admin',
            }
        },

        diagnose_node: {
            method: 'GET',
            params: {
                $ref: 'node_api#/definitions/node_identity'
            },
            reply: {
                type: 'string',
            },
            auth: {
                system: 'admin',
            }
        },

        update_n2n_config: {
            method: 'POST',
            params: {
                $ref: 'common_api#/definitions/n2n_config'
            },
            auth: {
                system: 'admin',
            }
        },

        update_base_address: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['base_address'],
                properties: {
                    base_address: {
                        type: 'string'
                    }
                }
            },
            auth: {
                system: 'admin',
            }
        },

        update_phone_home_config: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['proxy_address'],
                properties: {
                    proxy_address: {
                        anyOf: [{
                            type: 'null'
                        }, {
                            type: 'string'
                        }]
                    }
                }
            },
            auth: {
                system: 'admin',
            }
        },

        configure_remote_syslog: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['enabled'],
                properties: {
                    enabled: {
                        type: 'boolean'
                    },
                    protocol: {
                        type: 'string',
                        enum: ['TCP', 'UDP']
                    },
                    address: {
                        type: 'string'
                    },
                    port: {
                        type: 'number'
                    }
                }
            },
            auth: {
                system: 'admin',
            }
        },

        update_system_certificate: {
            method: 'POST',
            auth: {
                system: 'admin',
            }
        },

        phone_home_capacity_notified: {
            method: 'POST',
            auth: {
                system: 'admin',
            }
        },

        update_hostname: {
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
                system: 'admin',
            }
        },

        attempt_dns_resolve: {
            doc: 'Attempt to resolve a dns name',
            method: 'POST',
            params: {
                type: 'object',
                required: ['dns_name'],
                properties: {
                    dns_name: {
                        type: 'string'
                    }
                },
            },
            reply: {
                type: 'object',
                required: ['valid'],
                properties: {
                    valid: {
                        type: 'boolean'
                    },
                    reason: {
                        type: 'string'
                    }
                }
            },
            auth: {
                account: false,
                system: false,
            }
        },

        validate_activation: {
            method: 'GET',
            params: {
                type: 'object',
                required: ['code'],
                properties: {
                    code: {
                        type: 'string'
                    },
                    email: {
                        type: 'string'
                    }
                }
            },
            reply: {
                type: 'object',
                required: ['valid'],
                properties: {
                    valid: {
                        type: 'boolean',
                    },
                    reason: {
                        type: 'string'
                    }
                }
            },
            auth: {
                account: false,
                system: false,
            }
        },

        log_client_console: {
            method: 'POST',
            params: {
                type: 'object',
                required: ['data'],
                properties: {
                    data: {
                        type: 'array',
                        items: {
                            type: 'string'
                        }
                    },
                },
            },
            auth: {
                system: 'admin',
            }
        }
    },

    definitions: {

        system_info: {
            type: 'object',
            required: ['name'],
            properties: {
                name: {
                    type: 'string',
                },
            },
        },


        system_full_info: {
            type: 'object',
            required: [
                'name',
                'roles',
                'tiers',
                'pools',
                'storage',
                'nodes',
                'buckets',
                'objects',
                'owner',
            ],
            properties: {
                name: {
                    type: 'string',
                },
                roles: {
                    type: 'array',
                    items: {
                        $ref: '#/definitions/role_info'
                    }
                },
                owner: {
                    $ref: 'account_api#/definitions/account_info'
                },
                last_stats_report: {
                    format: 'idate',
                },
                tiers: {
                    type: 'array',
                    items: {
                        $ref: 'tier_api#/definitions/tier_info'
                    }
                },
                storage: {
                    $ref: 'common_api#/definitions/storage_info'
                },
                nodes: {
                    $ref: 'node_api#/definitions/nodes_aggregate_info'
                },
                buckets: {
                    type: 'array',
                    items: {
                        $ref: 'bucket_api#/definitions/bucket_info'
                    }
                },
                pools: {
                    type: 'array',
                    items: {
                        $ref: 'pool_api#/definitions/pool_extended_info'
                    },
                },
                accounts: {
                    type: 'array',
                    items: {
                        $ref: 'account_api#/definitions/account_info'
                    }
                },
                objects: {
                    type: 'integer'
                },
                ssl_port: {
                    type: 'string'
                },
                web_port: {
                    type: 'string'
                },
                web_links: {
                    type: 'object',
                    properties: {
                        agent_installer: {
                            type: 'string',
                        },
                        linux_agent_installer: {
                            type: 'string',
                        },
                        s3rest_installer: {
                            type: 'string',
                        },
                    }
                },
                maintenance_mode: {
                    type: 'object',
                    required: ['state'],
                    properties: {
                        state: {
                            type: 'boolean',
                        },
                        till: {
                            format: 'idate',
                        },
                    }
                },
                n2n_config: {
                    $ref: 'common_api#/definitions/n2n_config'
                },
                phone_home_config: {
                    type: 'object',
                    properties: {
                        proxy_address: {
                            anyOf: [{
                                type: 'null'
                            }, {
                                type: 'string'
                            }]
                        },
                        upgraded_cap_notification: {
                            type: 'boolean'
                        },
                        phone_home_unable_comm: {
                            type: 'boolean'
                        },
                    }
                },
                remote_syslog_config: {
                    type: 'object',
                    properties: {
                        protocol: {
                            type: 'string',
                            enum: ['TCP', 'UDP']
                        },
                        address: {
                            type: 'string'
                        },
                        port: {
                            type: 'number'
                        }
                    }
                },
                ip_address: {
                    type: 'string'
                },
                dns_name: {
                    type: 'string'
                },
                base_address: {
                    type: 'string'
                },
                version: {
                    type: 'string'
                },
                debug_level: {
                    type: 'integer'
                },
                system_cap: {
                    type: 'integer'
                },
                upgrade: {
                    type: 'object',
                    properties: {
                        status: {
                            type: 'string',
                            enum: ['CAN_UPGRADE', 'FAILED', 'PENDING', 'UNAVAILABLE']
                        },
                        message: {
                            type: 'string',
                        },
                    },
                },
                last_upgrade: {
                    format: 'idate'
                },
                cluster: {
                    $ref: '#/definitions/cluster_info'
                },
            },
        },

        role_info: {
            type: 'object',
            required: ['roles', 'account'],
            properties: {
                roles: {
                    type: 'array',
                    items: {
                        $ref: '#/definitions/role_enum'
                    }
                },
                account: {
                    type: 'object',
                    required: ['name', 'email'],
                    properties: {
                        name: {
                            type: 'string',
                        },
                        email: {
                            type: 'string',
                        },
                    }
                }
            }
        },


        role_enum: {
            enum: ['admin', 'user', 'viewer'],
            type: 'string',
        },


        cluster_info: {
            type: 'object',
            // required: ['count', 'online'],
            properties: {
                master_secret: {
                    type: 'string',
                },
                shards: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            shardname: {
                                type: 'string',
                            },
                            high_availabilty: {
                                type: 'boolean',
                            },
                            servers: {
                                type: 'array',
                                items: {
                                    $ref: '#/definitions/cluster_server_info'
                                }
                            }
                        }
                    }
                }
            }
        },

        cluster_server_info: {
            type: 'object',
            properties: {
                version: {
                    type: 'string'
                },
                secret: {
                    type: 'string',
                },
                status: {
                    type: 'string',
                    enum: ['CONNECTED', 'DISCONNECTED', 'IN_PROGRESS']
                },
                hostname: {
                    type: 'string'
                },
                address: {
                    type: 'string'
                },
                memory_usage: {
                    type: 'number'
                },
                storage: {
                    $ref: 'common_api#/definitions/storage_info'
                },
                cpu_usage: {
                    type: 'number'
                },
                location: {
                    type: 'string'
                },
                ntp_server: {
                    type: 'string'
                },
                time_epoch: {
                    format: 'idate'
                },
                timezone: {
                    type: 'string'
                },
                dns_servers: {
                    type: 'array',
                    items: {
                        type: 'string'
                    },
                },
                debug_level: {
                    type: 'integer'
                },
                services_status: {
                    $ref: '#/definitions/services_status'
                }
            }
        },

        services_status: {
            type: 'object',
            required: ['dns_status', 'ph_status'],
            properties: {
                dns_status: {
                    $ref: '#/definitions/service_status_enum'
                },
                ph_status: {
                    $ref: '#/definitions/service_status_enum'
                },
                dns_name: {
                    $ref: '#/definitions/service_status_enum'
                },
                ntp_status: {
                    $ref: '#/definitions/service_status_enum'
                },
                internet_connectivity: {
                    type: 'string',
                    enum: ['FAULTY']
                },
                proxy_status: {
                    $ref: '#/definitions/service_status_enum'

                },
                remote_syslog_status: {
                    $ref: '#/definitions/service_status_enum'
                },
                cluster_status: {
                    anyOf: [{
                        type: 'string',
                        enum: ['UNKNOWN']
                    }, {
                        type: 'array',
                        items: {
                            type: 'object',
                            required: ['secret', 'status'],
                            properties: {
                                secret: {
                                    type: 'string'
                                },
                                status: {
                                    $ref: '#/definitions/service_status_enum'
                                }
                            }
                        }
                    }]
                }
            }
        },

        service_status_enum: {
            type: 'string',
            enum: ['UNKNOWN', 'FAULTY', 'UNREACHABLE', 'OPERATIONAL']
        }
    }
};
