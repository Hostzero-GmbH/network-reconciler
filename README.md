# network-reconciler

A network configuration daemon that listens to `proxmox-eventbus` NATS messages (for VM migrations) and NetBox webhooks (for changes in the network documentation) in order to facilitate smooth transitions of VM specific network settings.

When a user creates an IP address in NetBox and assigns a VM with an inside IP to it, a webhook is sent to the active `network-reconciler` instances. If one of them contains the VM in question, the VM's internal IP and the public IP the user wants it exposed to are added to the NAT tables of the node, and the public IP is added as a route to the loopback interface of the node in order to tell the router where it currently resides.

## Requirements

- Proxmox cluster (tested with PVE 9)
- a router node with FRR
- each node running `network-reconciler` also needs a running `proxmox-eventbus` instance
- a NetBox v4 instance - though the service will still work if NetBox is down, a configuration URL and token are mandatory

## Installation

To deploy `network-reconciler` on multiple nodes easily, modify the `HOSTS` variable in the Makefile to include the wanted node addresses and run `make deploy`. If you'd rather deploy it manually, run `make package` then copy and install it on each node individually.

Once the service installation is done on a node, it still needs to be configured as per instructions in the installation output

```
  1. Edit /etc/network-reconciler/config.yaml — set nats.servers, netbox.url, enable webhooks
  2. Set the Netbox API token: echo "NR_NETBOX_TOKEN=<token>" >> /etc/network-reconciler/environment
  3. Configure FRR BGP — merge /usr/share/doc/network-reconciler/frr-bgp-example.conf
     into /etc/frr/frr.conf, then run: systemctl reload frr
     A node without `redistribute connected` announces nothing at all and its VMs
     are unreachable, with no error anywhere. Verify the RUNNING config with:
       vtysh -c "show running-config" | grep redistribute
  4. Run:  systemctl enable --now network-reconciler
  5. Check: journalctl -u network-reconciler -f
```

## Operation

### Via NetBox

In the Virtual Machines section of NetBox, create a VM and assign the VMID (under Custom Fields, you may need to add it if you haven't already). Then, under IP Addresses, create the IP which is used within the VM and assign it to the VM. Finally, create the IP you want the VM exposed under and set the internal VM IP as the `NAT (inside)` setting.

When the config is created, webhooks are sent out and you should see the updated network settings. To check NAT settings, run

```
nft list ruleset
```

and you should see something like this (but with the actual IPs)

```
table inet network-reconciler {
        chain prerouting {
                type nat hook prerouting priority dstnat; policy accept;
                ip daddr $EXTERNAL_IP dnat ip to $INTERNAL_IP
        }

        chain postrouting {
                type nat hook postrouting priority srcnat; policy accept;
                ip saddr $INTERNAL_IP snat ip to $EXTERNAL_IP
        }
}
```

To check if the route was set to the loopback interface, run

```
ip -4 addr show dev lo
```

the expected output would be

```
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
    inet $EXTERNAL_IP/32 scope global lo
       valid_lft forever preferred_lft forever
```

### Via config files

All VM configurations that `network-reconciler` receives are stored in the shared directory `/etc/pve/network-reconciler` so that if NetBox is unavailable, VM network settings can still be applied. This means that you can also configure a VM without adding NetBox objects by creating a `$VMID.config` file in the shared directory. The file should look like this:

```
{
  "ExternalIP": "$EXTERNAL_IP",
  "InternalIP": "$INTERNAL_IP",
  "VMProxmoxVMID": $VMID,
  "VMName": "$VM_NAME"
}
```

To check that the settings are properly applied, use the same commands as for NetBox.

## License

Apache-2.0. See [LICENSE](LICENSE).
