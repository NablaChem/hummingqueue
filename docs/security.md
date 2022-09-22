There is no protection against a malicious *Owner* in the *Collaboration*.

# What is the trust model?

All compute *Nodes* of an *Owner* are assumed to operate in good faith to that *Owner* or to any *Collaboration* they are part of. This is modeled along the lines of existing queueing systems, where the *Nodes* need to read and write code and data while their results are generally trusted.

*Users* are generally untrusted with the exception of the *Owner*. They are, however, assumed to not sabotage themselves.

The *Queue* does not get any information that could lead to compromised results. It is a broker of *Jobs* only, not knowing what the individual jobs are about. It could, however, sabotage the calculations e.g. by not scheduling them, just like existing queueing systems.

# What happens if a *hummingqueue* installation gets compromised?

The data on S3 storage is protected against read access as the *Queue* only stores these credentials in an encrypted manner without ever knowing the key. Attackers could start a denial-of-service attack against the *Nodes*  by having them perform useless work based on replay data but they cannot submit arbitrary code as the *Nodes* check for a valid authorized signature of the *Job*.

# How are permissions granted to *Users* such that attackers cannot send malicious *Jobs*?

*Owners* need to be created by the *Queue*, typically via some sign-in connector or external tool. This requires the *Owner* to having created a public/private key pair to validate incoming jobs and secure the *Owners* compute infrastructure. This way, a compromised *Queue* cannot give access to the compute *nodes*.

Every potential *User* creates such a public/private key pair and sends the public key to the *Owner* who signs it and uploads that signature into the *Queue*, thus authorizing that *User* to submit *Jobs* to that *Owners* infrastructure. This permission is checked once more before the *Job* starts. The fact that the *Owner* never knows the private key of the *User* allows the *Owner* to prove that a certain *Job* has been submitted by a given *User*.

Authorizations are revoked by signing the recovation and uploading it to the *Queue*. This is cached by every *Node* of that *Owner*. Reinstating access will require the creation of a new *User*.

# What happens if a compute node is compromised?

In this case, the public/private key pair for that *Owner* needs to be replaced, as all *Nodes* share that information.

# In a collaboration, how are the credentials to S3 shared securely?

In a nutshell: the central database only holds encrypted S3 credentials. Knowing everything in the database is not enough to gain access to the S3 data or to write to it.

The following describes the technical details. Every owner has a [public/private RSA key pair](https://en.wikipedia.org/wiki/Public-key_cryptography), the public key of which is stored in the central database and is linked to the *Token* of the *Owner*, a system generated identifier of the owner. The private key needs to be available on each compute *Node* of that *Owner*. 

- If one owner joins another project, their public key is linked to the project and can be downloaded by every project member. The time of joining is recorded.
- Upon job submission, the credentials for this particular job (as they can differ per job) are encrypted with all public keys of all current project members. The server stores time of submission and only sends jobs to owners who had joined prior to the submission time.
- Should a owner be evicted by others in the project, their nodes will not get any further jobs to execute, although they would be able to decrypt the credentials of all jobs submitted prior to their eviction should the database be compromised.
- Once the job is completed successfully, the encrypted credentials are removed from the database.

# How to cycle the *Owner* private key?
This will require cooperation from the admins of the installation to verify the actual owner, e.g. via a email challenge. This needs to be implemented by the operator of the installation and is not scope of this software.